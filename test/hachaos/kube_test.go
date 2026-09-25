//go:build linux

package hachaos

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// kubeAPI is a real kube-apiserver backed by etcd. Candidates reach it through
// a fault proxy with the in-cluster service-account configuration they use in
// a pod, and are authorized only for the Lease permissions the documentation
// lists.
type kubeAPI struct {
	assets   string
	dir      string
	logs     string
	timeline *timeline

	etcd       *process
	apiserver  *process
	etcdPort   int
	apiPort    int
	proxy      *proxy
	ca         []byte
	adminToken string
	client     *kubernetes.Clientset
}

const (
	leaseNamespace = "matrix"
	leaseName      = "hachaos"
	serviceAccount = "zendrite"
	// InClusterDir is where client-go's in-cluster configuration looks. It is
	// a constant in client-go, so the harness must be able to write it, which
	// in practice means running as root inside a container.
	inClusterDir = "/var/run/secrets/kubernetes.io/serviceaccount"
)

func findKubeAssets() (string, error) {
	dir := os.Getenv("HACHAOS_KUBE_ASSETS")
	if dir == "" {
		return "", errors.New("HACHAOS_KUBE_ASSETS must name a directory containing etcd and kube-apiserver")
	}
	for _, name := range []string{"etcd", "kube-apiserver"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return "", fmt.Errorf("kube assets: %w", err)
		}
	}
	return dir, nil
}

func startKube(ctx context.Context, dir, logs string, o *owners, tl *timeline) (*kubeAPI, error) {
	assets, err := findKubeAssets()
	if err != nil {
		return nil, err
	}
	k := &kubeAPI{assets: assets, dir: dir, logs: logs, timeline: tl}
	if err = k.writeCredentials(); err != nil {
		return nil, err
	}
	if k.etcdPort, err = freePort(); err != nil {
		return nil, err
	}
	if k.apiPort, err = freePort(); err != nil {
		return nil, err
	}
	if err = k.startEtcd(ctx); err != nil {
		return nil, err
	}
	if err = k.startAPIServer(ctx); err != nil {
		return nil, err
	}
	if k.proxy, err = newProxy("kube-apiserver", func() string { return fmt.Sprintf("127.0.0.1:%d", k.apiPort) }, o, tl); err != nil {
		return nil, err
	}
	if err = k.authorize(ctx); err != nil {
		return nil, err
	}
	return k, nil
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// writeCredentials creates the serving certificate, the service-account
// signing key, static bearer tokens, and the in-cluster files candidates read.
func (k *kubeAPI) writeCredentials() error {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "hachaos-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	serveKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	serveTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "kube-apiserver"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost", "kubernetes.default.svc"},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serveDER, err := x509.CreateCertificate(rand.Reader, serveTemplate, caCert, &serveKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	saKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	saPub, err := x509.MarshalPKIXPublicKey(&saKey.PublicKey)
	if err != nil {
		return err
	}
	k.ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	k.adminToken = randomToken()
	candidateToken := randomToken()
	files := map[string][]byte{
		"ca.crt":     k.ca,
		"serve.crt":  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serveDER}),
		"serve.key":  pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serveKey)}),
		"sa.key":     pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(saKey)}),
		"sa.pub":     pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: saPub}),
		"tokens.csv": fmt.Appendf(nil, "%s,hachaos-admin,hachaos-admin,\"system:masters\"\n%s,system:serviceaccount:%s:%s,%s,\"system:serviceaccounts,system:serviceaccounts:%s\"\n", k.adminToken, candidateToken, leaseNamespace, serviceAccount, serviceAccount, leaseNamespace),
	}
	for name, body := range files {
		if err = os.WriteFile(filepath.Join(k.dir, name), body, 0o600); err != nil {
			return err
		}
	}
	if err = os.MkdirAll(inClusterDir, 0o755); err != nil {
		return fmt.Errorf("the harness must be able to write %s (run it as root in a container): %w", inClusterDir, err)
	}
	for name, body := range map[string][]byte{"token": []byte(candidateToken), "ca.crt": k.ca, "namespace": []byte(leaseNamespace)} {
		if err = os.WriteFile(filepath.Join(inClusterDir, name), body, 0o644); err != nil {
			return fmt.Errorf("write in-cluster credentials: %w", err)
		}
	}
	return nil
}

func (k *kubeAPI) startEtcd(ctx context.Context) error {
	peer, err := freePort()
	if err != nil {
		return err
	}
	client := fmt.Sprintf("http://127.0.0.1:%d", k.etcdPort)
	peerURL := fmt.Sprintf("http://127.0.0.1:%d", peer)
	proc, err := startProcess(k.logs, processSpec{name: "etcd", bin: filepath.Join(k.assets, "etcd"), args: []string{
		"--data-dir", filepath.Join(k.dir, "etcd"),
		"--listen-client-urls", client, "--advertise-client-urls", client,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "default=" + peerURL,
	}})
	if err != nil {
		return err
	}
	k.etcd = proc
	return waitFor(ctx, time.Minute, func() error {
		if proc.exited() {
			return errPermanent(errors.New("etcd exited during startup"))
		}
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", k.etcdPort), time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	})
}

func (k *kubeAPI) startAPIServer(ctx context.Context) error {
	proc, err := startProcess(k.logs, processSpec{name: "kube-apiserver", bin: filepath.Join(k.assets, "kube-apiserver"), args: []string{
		fmt.Sprintf("--etcd-servers=http://127.0.0.1:%d", k.etcdPort),
		"--bind-address=127.0.0.1", "--advertise-address=127.0.0.1",
		fmt.Sprintf("--secure-port=%d", k.apiPort),
		"--cert-dir=" + filepath.Join(k.dir, "certs"),
		"--tls-cert-file=" + filepath.Join(k.dir, "serve.crt"),
		"--tls-private-key-file=" + filepath.Join(k.dir, "serve.key"),
		"--token-auth-file=" + filepath.Join(k.dir, "tokens.csv"),
		"--authorization-mode=RBAC",
		fmt.Sprintf("--service-account-issuer=https://127.0.0.1:%d", k.apiPort),
		"--service-account-key-file=" + filepath.Join(k.dir, "sa.pub"),
		"--service-account-signing-key-file=" + filepath.Join(k.dir, "sa.key"),
		"--service-cluster-ip-range=10.0.0.0/24",
		"--disable-admission-plugins=ServiceAccount",
		// A loopback advertise address cannot back the kubernetes Service.
		"--endpoint-reconciler-type=none",
	}})
	if err != nil {
		return err
	}
	k.apiserver = proc
	k.timeline.add(eventProc, "kube-apiserver", "started")
	cfg := k.adminConfig()
	if k.client, err = kubernetes.NewForConfig(cfg); err != nil {
		return err
	}
	return waitFor(ctx, 2*time.Minute, func() error {
		if proc.exited() {
			return errPermanent(errors.New("kube-apiserver exited during startup"))
		}
		result := k.client.Discovery().RESTClient().Get().AbsPath("/readyz").Do(ctx)
		return result.Error()
	})
}

func (k *kubeAPI) adminConfig() *rest.Config {
	return &rest.Config{
		Host:            fmt.Sprintf("https://127.0.0.1:%d", k.apiPort),
		BearerToken:     k.adminToken,
		TLSClientConfig: rest.TLSClientConfig{CAData: k.ca},
		Timeout:         10 * time.Second,
	}
}

// authorize grants exactly the documented permissions: get, create and update
// on Leases in the configured namespace.
func (k *kubeAPI) authorize(ctx context.Context) error {
	if _, err := k.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: leaseNamespace}}, metav1.CreateOptions{}); err != nil {
		return err
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "zendrite-ha", Namespace: leaseNamespace},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "create", "update"}}},
	}
	if _, err := k.client.RbacV1().Roles(leaseNamespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "zendrite-ha", Namespace: leaseNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: fmt.Sprintf("system:serviceaccount:%s:%s", leaseNamespace, serviceAccount)}},
	}
	_, err := k.client.RbacV1().RoleBindings(leaseNamespace).Create(ctx, binding, metav1.CreateOptions{})
	return err
}

func deleteOptions() metav1.DeleteOptions { return metav1.DeleteOptions{} }

// candidateEnv points client-go's in-cluster configuration at the proxy.
func (k *kubeAPI) candidateEnv() []string {
	return []string{"KUBERNETES_SERVICE_HOST=127.0.0.1", "KUBERNETES_SERVICE_PORT=" + k.proxy.port()}
}

func (k *kubeAPI) stop() {
	if k.proxy != nil {
		k.proxy.close()
	}
	if k.apiserver != nil {
		k.apiserver.stop(10 * time.Second)
	}
	if k.etcd != nil {
		k.etcd.stop(10 * time.Second)
	}
}
