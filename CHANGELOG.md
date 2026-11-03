# Changelog

## 0.1.0 (2026-11-03)

### .

#### Breaking

- **isolate**: drive a remote Docker daemon from the docker driver (#21) ([f826ef0](https://github.com/urmzd/orchagent/commit/f826ef0dbeab64b85e5c8323f3d7ea132abdc936))
- **isolate**: give each instance its own /project and /me workspaces (#20) ([56e6890](https://github.com/urmzd/orchagent/commit/56e6890b45779c8b6bdd0787f193754c6dc4f55a))
- **isolate**: require gVisor and a stricter seccomp profile in the docker driver (#19) ([1d05cac](https://github.com/urmzd/orchagent/commit/1d05cacddd225b0db3f7c211ffc3f70427b2465c))
- agent experience in the console, Google models and provider health (#110) ([ec5a878](https://github.com/urmzd/orchagent/commit/ec5a878733675d851dd784b559e640de38be77da))
- Humagen Go on the event union ([933074b](https://github.com/urmzd/orchagent/commit/933074b15fdc5ee94e44d11ec42d749f6876aeac))
- playscape is a library with a playscape run CLI; the server and its queue are gone (#12) ([7ec8b1f](https://github.com/urmzd/orchagent/commit/7ec8b1f5257fd57dbfa62c741437aac5ce4002a6))
- rename duraturo to anchor (#3) ([af2849a](https://github.com/urmzd/orchagent/commit/af2849abc08ddd2c6b205694c3fd83092128e5c2))
- rename legatus to playscape (#6) ([016a0fa](https://github.com/urmzd/orchagent/commit/016a0fab5796098dfafef2a5c0ab94e3cb175add))
- rename project from dispatch to legatus (#4) ([dc29f03](https://github.com/urmzd/orchagent/commit/dc29f03b246977b461b0b8054e761bc3139bdf87))

#### Features

- **notice**: watch notices reach every replica through Postgres NOTIFY (#177) ([d35e1ea](https://github.com/urmzd/orchagent/commit/d35e1ea6543fa8e26a0923543a90377d2757606a))
- **stores**: every store reads and writes per request (RFC 0004, change 9) (#176) ([5cf6106](https://github.com/urmzd/orchagent/commit/5cf610604ab4b8b0f0b4bb5e33908e064ffef107))
- **bus**: per-tenant consumer groups for egress, the observer and dispatch (RFC 0006) (#162) ([718c8f9](https://github.com/urmzd/orchagent/commit/718c8f973d48a3c6cdbc99aa86770d935383c156))
- **fiber**: let a host park a run on a question of its own (#153) ([d85bf63](https://github.com/urmzd/orchagent/commit/d85bf63b5bafec002b330a51566e60c622e9f410))
- install a run's memory on lease and keep it on release (RFC 0004) (#152) ([0977cd4](https://github.com/urmzd/orchagent/commit/0977cd45a0789f846cf8d958e5454e2564bea647))
- **sandbox**: egress gate so a run's tools reach allowed hosts (#151) ([c85a9db](https://github.com/urmzd/orchagent/commit/c85a9dbf1b611fa705776506f15955482c209823))
- **knowledge**: hybrid search with a configurable embedder (RFC 0005, change 1) (#132) ([dd371b2](https://github.com/urmzd/orchagent/commit/dd371b2a148af3e723b699e62e8aedb13190ce3f))
- run a run's tools in its isolated instance (RFC 0004, layer 4) (#127) ([6188cdf](https://github.com/urmzd/orchagent/commit/6188cdfb0dd3fb73579b093f4e7b21bc4312b725))
- run context, person store and setting kinds (RFC 0004, change 5) (#122) ([1aeea85](https://github.com/urmzd/orchagent/commit/1aeea8580e568ac26e167e2e330e614776ac1c42))
- **isolate**: add the egress gate with credential injection (#18) ([f076cae](https://github.com/urmzd/orchagent/commit/f076cae661dd10beba363c7e8724594a0938b3a3))
- **isolate**: run a run's tools in pooled, isolated instances (#17) ([643426c](https://github.com/urmzd/orchagent/commit/643426c88d64776aba5734415c8c221de25d1331))
- **agents**: tenant definition store on pkg/state with conversion ([f86ab5a](https://github.com/urmzd/orchagent/commit/f86ab5a7b7d7f9c826519944f0d49f57835cd718))
- **postgres**: anchor CLI to inspect and replay a ledger (#6) ([73c9988](https://github.com/urmzd/orchagent/commit/73c9988015a736fa52e7f7e461b6d7f67099647a))
- **postgres**: pgkit for migrations and LISTEN, a ledger prefix read, byte-ordered keys (#5) ([ecb6802](https://github.com/urmzd/orchagent/commit/ecb68021d41d049b60cec6d6f2d93e97b88a0dae))
- **tool**: let a runtime check a spawn without dispatching it (#10) ([3072842](https://github.com/urmzd/orchagent/commit/3072842b3b39075f6738216ff0ad554a2c6d1581))
- **runner**: add Job-backed sandboxed shell executor (#9) ([5c2e4af](https://github.com/urmzd/orchagent/commit/5c2e4af1a15af290c0e49bd71a84957bda8b23b2))
- **tool**: add cli provider kind wrapping CLI binaries (#8) ([ae6aaaf](https://github.com/urmzd/orchagent/commit/ae6aaaf36e0af14b55db59777532e9d45faea969))
- **pgqueue**: Postgres DeltaLog for streaming without Redis (#4) ([721f584](https://github.com/urmzd/orchagent/commit/721f58449effd44c210e2fe8f750598b2cda5b14))
- duraturo, durable execution for existing Go code ([8fb268a](https://github.com/urmzd/orchagent/commit/8fb268a398920b2e0b34517a8c3b6b34e1475688))
- define access with an NGAC policy machine ([06109ba](https://github.com/urmzd/orchagent/commit/06109ba3f5b6627f33022083f3327ef1484c91bb))

#### Bug Fixes

- **flow/runschema**: name the package and its errors runschema ([995904e](https://github.com/urmzd/orchagent/commit/995904e147ae33a5936c8ad7bb60a591e4a1128b))
- feedback prompt capture, configured-runtime compaction budget, held-call docs (#149) ([e045668](https://github.com/urmzd/orchagent/commit/e045668c6e2cd5a12cef90ccb43dfc4d1e7c34c0))
- **turn/native**: fold a compact request that arrived before the next model call (#136) ([3b10e27](https://github.com/urmzd/orchagent/commit/3b10e27a36bca620d26afcb69983defe8b9b3305))
- **deps**: patch vulnerable Go dependencies (#10) ([bfa0a10](https://github.com/urmzd/orchagent/commit/bfa0a10ba95c43c3424f415b807c2e0fbe8083f8))
- **deps**: patch vulnerable Go dependencies (#16) ([01315fa](https://github.com/urmzd/orchagent/commit/01315fab657909d923d450686cf7b5492ffcaf65))
- **fiber/definition**: stamp records at microsecond precision so a Postgres read matches the write ([9de5232](https://github.com/urmzd/orchagent/commit/9de52329aa5dd55d61e73d6fe984529304bd04c4))
- keep call depth within the hop limit, read old journal rows and agent tables, bump the manifest to v2 ([b17c013](https://github.com/urmzd/orchagent/commit/b17c01392bbfb63bcd041283b25e9c68e7f900f6))
- a stop mid-claim no longer leases a Postgres run to nobody (#7) ([30cc1cf](https://github.com/urmzd/orchagent/commit/30cc1cfff3ecdaf221208c6527b4c431c33c17a2))
- **k8s**: require playscape v0.7.0, the first release without the package (#13) ([09a8a4b](https://github.com/urmzd/orchagent/commit/09a8a4b4f786547e94b9ceaa66600f4b46e992d6))
- exact CLI arguments, Kubernetes expansion and timeouts, process-group cancel (#11) ([0b6a9eb](https://github.com/urmzd/orchagent/commit/0b6a9eb2c562cba8fddaec69ec25fd8f2287d894))
- **ci**: tidy example modules against the v0.1.0 adapter pins ([ff867d0](https://github.com/urmzd/orchagent/commit/ff867d0b00f8215e4cb92edd91ea0dba138d4f0e))

#### Misc

- merge humagen history ([0334fa5](https://github.com/urmzd/orchagent/commit/0334fa599feecb19007297c18b5595e4f9ae9f40))
- merge playscape history ([7ed21fc](https://github.com/urmzd/orchagent/commit/7ed21fc6059ee76e7b9b69e3b439481281a5a5e9))
- merge anchor history ([97340b4](https://github.com/urmzd/orchagent/commit/97340b4c0a7b9758593fbb8564cf8778fa108721))
- start orchagent ([e96e4ae](https://github.com/urmzd/orchagent/commit/e96e4aeb5697e9c6b14e7603d4559c2b24cd998d))
- **deps**: bump playscape to v0.10.0 (#137) ([fb52d31](https://github.com/urmzd/orchagent/commit/fb52d31e7dbba7c71248ee6a29032697be179402))
- **legacy**: one home for old names after the fold ([fc98559](https://github.com/urmzd/orchagent/commit/fc98559451a17c07808d9aa30e8d0a0e1ccd1c01))
- fold fiber into fiber/ with history ([67f4f21](https://github.com/urmzd/orchagent/commit/67f4f214ff87570b1ff464d9c5fc10687fdf17c8))
- add fiber at fiber/ with history ([14526ed](https://github.com/urmzd/orchagent/commit/14526ed8d6aad6b196b9702084782ce6a5dc5eb4))
- **deps**: upgrade saige example to v0.37.0 (#5) ([39d1f87](https://github.com/urmzd/orchagent/commit/39d1f87845056cdf71664734c4b48da39c9c9fa5))
- **release**: read the sr-releaser key from the release environment (#2) ([45348c1](https://github.com/urmzd/orchagent/commit/45348c19e23ecd3e75cb8105397bf42e2f470557))
- **release**: read the sr-releaser key from the release environment (#3) ([7a7e9f5](https://github.com/urmzd/orchagent/commit/7a7e9f5ebf9a80cf2648b9d6d2a12cad49765301))
- upgrade urmzd/sr action to v9 (#1) ([30e5464](https://github.com/urmzd/orchagent/commit/30e5464b9ba204f1b714227d423a89217349f38d))
- upgrade urmzd/sr action to v9 (#2) ([e7ec767](https://github.com/urmzd/orchagent/commit/e7ec767edcf644177f6828941653cba94ae2df67))
- pin adapters to duraturo v0.1.0 ([269c59b](https://github.com/urmzd/orchagent/commit/269c59bc92ab0c65c598b64c1d50f4724567a460))
- initial project scaffold ([8a62865](https://github.com/urmzd/orchagent/commit/8a62865204e15c403a357f48fa8aaaa6e915f26e))

### sandbox/runner/k8s
