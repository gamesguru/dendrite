# Dendrite

Dendrite is a second-generation Matrix homeserver written in Go.  
**This repository has been forked from [element-hq/dendrite](https://github.com/element-hq/dendrite) in 2026-02.**

It intends to provide an **efficient** and **reliable** alternative to [Synapse](https://github.com/matrix-org/synapse):

- Efficient: A small memory footprint with better baseline performance than an out-of-the-box Synapse.
- Reliable: Implements the Matrix specification as written, using the
  [same](https://github.com/matrix-org/sytest) test [suites](https://github.com/matrix-org/complement) as Synapse.

Dendrite is currently lacking support for the following MSCs:

- [MSC3861](https://github.com/matrix-org/matrix-spec-proposals/pull/3861): Next-gen auth OIDC

## Installation

### Container Images

Container images are available on

- [Docker Hub](https://hub.docker.com/r/pats22/dendrite)
- [CodeFloe Container Registry](https://codefloe.com/pat-s/dendrite)

The first public release of this fork was v0.15.3.
Please check [element-hq/dendrite](https://github.com/element-hq/dendrite) for older versions

### Binaries

Binaries are attached to their respective [releases](https://codefloe.com/pat-s/dendrite/releases).

## Copyright & License

Copyright 2017 OpenMarket Ltd  
Copyright 2017 Vector Creations Ltd  
Copyright 2017-2025 New Vector Ltd

This software is dual-licensed by New Vector Ltd (Element). It can be used either:

(1) for free under the terms of the GNU Affero General Public License (as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version); OR

(2) under the terms of a paid-for Element Commercial License agreement between you and Element (the terms of which may vary depending on what you and Element have agreed to).
Unless required by applicable law or agreed to in writing, software distributed under the Licenses is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Licenses for the specific language governing permissions and limitations under the Licenses.
