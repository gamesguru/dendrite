package routing

import "testing"

func TestAutomaticSubscriptionConflicts(t *testing.T) {
	const threadID = "$thread"

	tests := []struct {
		name      string
		data      threadSubscriptionsData
		bumpOrder int64
		bumpStamp int64
		want      bool
	}{
		{
			name: "local ordering decides when both sides have it",
			data: threadSubscriptionsData{
				Unsubscribed:      map[string]int64{threadID: 2_000},
				UnsubscribedOrder: map[string]int64{threadID: 20},
			},
			bumpOrder: 10,
			bumpStamp: 3_000,
			want:      true,
		},
		{
			name: "local ordering can override a later timestamp",
			data: threadSubscriptionsData{
				Unsubscribed:      map[string]int64{threadID: 2_000},
				UnsubscribedOrder: map[string]int64{threadID: 20},
			},
			bumpOrder: 30,
			bumpStamp: 1_000,
			want:      false,
		},
		{
			name: "legacy document uses timestamps",
			data: threadSubscriptionsData{
				Unsubscribed: map[string]int64{threadID: 2_000},
			},
			bumpOrder: 10,
			bumpStamp: 1_500,
			want:      true,
		},
		{
			name: "mixed document falls back to timestamps when the cause has no ordering",
			data: threadSubscriptionsData{
				Unsubscribed:      map[string]int64{threadID: 2_000},
				UnsubscribedOrder: map[string]int64{threadID: 20},
			},
			bumpOrder: 0,
			bumpStamp: 1_500,
			want:      true,
		},
		{
			name:      "no unsubscription",
			data:      threadSubscriptionsData{},
			bumpOrder: 10,
			bumpStamp: 3_000,
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := automaticSubscriptionConflicts(tc.data, threadID, tc.bumpOrder, tc.bumpStamp); got != tc.want {
				t.Fatalf("automaticSubscriptionConflicts() = %v, want %v", got, tc.want)
			}
		})
	}
}
