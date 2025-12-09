## [2.5.0](https://github.com/efcasado/pulsar-elixir/compare/v2.4.0...v2.5.0) (2025-12-09)

### Features

* compacted consumers ([#92](https://github.com/efcasado/pulsar-elixir/issues/92)) ([871f92c](https://github.com/efcasado/pulsar-elixir/commit/871f92c3e7d3e7dac80c6bdb5ac587cc0ad289e1))

## [2.4.0](https://github.com/efcasado/pulsar-elixir/compare/v2.3.0...v2.4.0) (2025-12-09)

### Features

* support message chunking ([#91](https://github.com/efcasado/pulsar-elixir/issues/91)) ([59c56c8](https://github.com/efcasado/pulsar-elixir/commit/59c56c8631500c424fb9d4053b329e70993bcbff))

## [2.3.0](https://github.com/efcasado/pulsar-elixir/compare/v2.2.7...v2.3.0) (2025-12-06)

### Features

* partitioned producers ([#82](https://github.com/efcasado/pulsar-elixir/issues/82)) ([4716ae5](https://github.com/efcasado/pulsar-elixir/commit/4716ae53a76f02f78c2d59b681094ef952579e52))

## [2.2.7](https://github.com/efcasado/pulsar-elixir/compare/v2.2.6...v2.2.7) (2025-12-06)

## [2.2.6](https://github.com/efcasado/pulsar-elixir/compare/v2.2.5...v2.2.6) (2025-12-05)

### Bug Fixes

* **build:** wrong protoc-gen-elixir path ([099b4b6](https://github.com/efcasado/pulsar-elixir/commit/099b4b633bac78d8e49156c9ce45e0dab2151c58))
* regenerate protobuf models ([e9c4225](https://github.com/efcasado/pulsar-elixir/commit/e9c42253184e03db060a7c85be4244171a3fc633))

## [2.2.5](https://github.com/efcasado/pulsar-elixir/compare/v2.2.4...v2.2.5) (2025-12-05)

## [2.2.4](https://github.com/efcasado/pulsar-elixir/compare/v2.2.3...v2.2.4) (2025-12-04)

## [2.2.3](https://github.com/efcasado/pulsar-elixir/compare/v2.2.2...v2.2.3) (2025-12-04)

### Bug Fixes

* better ssl defaults using castore ([1c20a54](https://github.com/efcasado/pulsar-elixir/commit/1c20a54e70eb0b80720a3cfe97985af4578332ae))

## [2.2.2](https://github.com/efcasado/pulsar-elixir/compare/v2.2.1...v2.2.2) (2025-12-04)

## [2.2.1](https://github.com/efcasado/pulsar-elixir/compare/v2.2.0...v2.2.1) (2025-12-03)

### Bug Fixes

* **broadway:** stale consumer when using DLQ policy ([b03d3f8](https://github.com/efcasado/pulsar-elixir/commit/b03d3f81e46b2703c7c7fe908836c00cbbbe7ee1))

## [2.2.0](https://github.com/efcasado/pulsar-elixir/compare/v2.1.1...v2.2.0) (2025-12-03)

### Features

* **producer:** support additional delivery options ([#76](https://github.com/efcasado/pulsar-elixir/issues/76)) ([fcff5ae](https://github.com/efcasado/pulsar-elixir/commit/fcff5ae3ccecc66f1fe0351eb2d572d258bd521f))

## [2.1.1](https://github.com/efcasado/pulsar-elixir/compare/v2.1.0...v2.1.1) (2025-12-03)

## [2.1.0](https://github.com/efcasado/pulsar-elixir/compare/v2.0.0...v2.1.0) (2025-12-01)

### Features

* producer access modes ([#38](https://github.com/efcasado/pulsar-elixir/issues/38)) ([b0eebbb](https://github.com/efcasado/pulsar-elixir/commit/b0eebbb03cce4af6fe13b3f143818fe763091ee8))

## [2.0.0](https://github.com/efcasado/pulsar-elixir/compare/v1.2.1...v2.0.0) (2025-11-30)

### ⚠ BREAKING CHANGES

* use struct for consumed messages

### Features

* use struct for consumed messages ([8b0b6cd](https://github.com/efcasado/pulsar-elixir/commit/8b0b6cd2f451b7135389d072fb58aa8ed94c70d4))

## [1.2.1](https://github.com/efcasado/pulsar-elixir/compare/v1.2.0...v1.2.1) (2025-11-29)

## [1.2.0](https://github.com/efcasado/pulsar-elixir/compare/v1.1.0...v1.2.0) (2025-11-29)

### Features

* exec callback's init upon successful subscription ([0dd20ee](https://github.com/efcasado/pulsar-elixir/commit/0dd20ee06e6cf73ea9cf6ef8f7e61f9ffaa9afbc))
* graceful client shutdown ([ff6b1ec](https://github.com/efcasado/pulsar-elixir/commit/ff6b1ec014f549087e0be5d19247fcc30007cfce))

### Bug Fixes

* broker api does not support non-default clients ([e4fbf76](https://github.com/efcasado/pulsar-elixir/commit/e4fbf7603ca378ad4c9ae1573bbdc6c220be875e))
* dlq producer started within wrong client ([7019c32](https://github.com/efcasado/pulsar-elixir/commit/7019c32f3746c180123c0438cc03dce8c91ef331))

## [1.1.0](https://github.com/efcasado/pulsar-elixir/compare/v1.0.1...v1.1.0) (2025-11-24)

### Features

* promote consumer callback behavior to a macro ([c27a183](https://github.com/efcasado/pulsar-elixir/commit/c27a183eecd5a979d4fbd570ee07919f1392c9d2))

## [1.0.1](https://github.com/efcasado/pulsar-elixir/compare/v1.0.0...v1.0.1) (2025-11-23)

## 1.0.0 (2025-11-23)

### ✨ Features

* add support for a variety of subscription options ([#21](https://github.com/efcasado/pulsar-elixir/issues/21)) ([c0e351d](https://github.com/efcasado/pulsar-elixir/commit/c0e351deac411939f3c9725384f801d2965a4ff5))
* add support for DLQ topics ([f3d6885](https://github.com/efcasado/pulsar-elixir/commit/f3d6885ab4e01752fbb165008f50c23d6edcc7a8))
* add support for nacking messages ([74e516f](https://github.com/efcasado/pulsar-elixir/commit/74e516f1840cdacb47d259c441ba2c56c4dbd231))
* add support for partitioned topics ([8928ba8](https://github.com/efcasado/pulsar-elixir/commit/8928ba85ed120fa652de3fea20ec43c162b8a141))
* basic producer support ([b35a627](https://github.com/efcasado/pulsar-elixir/commit/b35a627e9c272ce91a6e2dc9d091c9e34c1dd90d))
* batch acknowledgements ([ec1ef27](https://github.com/efcasado/pulsar-elixir/commit/ec1ef278ad1ff6e83c30d3a723072db3d6d72234))
* broadway support ([#48](https://github.com/efcasado/pulsar-elixir/issues/48)) ([3b4f162](https://github.com/efcasado/pulsar-elixir/commit/3b4f162a0c8e69506e33cc81c1bf9eb87e1f8643))
* broker-initiated consumer and producer closures ([dc8ce61](https://github.com/efcasado/pulsar-elixir/commit/dc8ce6106549cc895dce9454eaecc906f0394782))
* configurable flow control ([ce608dd](https://github.com/efcasado/pulsar-elixir/commit/ce608ddb1249b6f7fe24452448cdd3c4c700be83))
* consumer group and partitioned consumer abstractions ([#36](https://github.com/efcasado/pulsar-elixir/issues/36)) ([61db170](https://github.com/efcasado/pulsar-elixir/commit/61db1706774ce2f90335c137dff3fcae9dea8ed8))
* consumer groups ([a8783f6](https://github.com/efcasado/pulsar-elixir/commit/a8783f63254c3b5d3b08e14857fcab7cee0e3640))
* graceful shutdown ([a1973d0](https://github.com/efcasado/pulsar-elixir/commit/a1973d014092a33f643934956ca3f81ddaebe600))
* multi-client architecture ([#50](https://github.com/efcasado/pulsar-elixir/issues/50)) ([33687a0](https://github.com/efcasado/pulsar-elixir/commit/33687a05ab634f1c28ee8dadfaeb3e426b6ddcf0))
* support batched messages in consumers ([f8fef69](https://github.com/efcasado/pulsar-elixir/commit/f8fef6983f4a9cb7f60b5a58e682f2af5f1cf7e0))
* support message compression ([ba84919](https://github.com/efcasado/pulsar-elixir/commit/ba84919ad9dc4053d2baa57d992a05fb9c773151))

### 🐛 Bug Fixes

* async acking and flow control ([7eaba3c](https://github.com/efcasado/pulsar-elixir/commit/7eaba3c13aceb55a2f591affad02fc6b50641575))
* async acknowledgements ([1ac4fe1](https://github.com/efcasado/pulsar-elixir/commit/1ac4fe19d4419a72156ffe63984e6f49c1479a91))
* async manual acks ([c70e5b0](https://github.com/efcasado/pulsar-elixir/commit/c70e5b00cdf06e99195703c38bcd0a5dccae6d08))
* broker not ready for for partitioned topic check ([cba0096](https://github.com/efcasado/pulsar-elixir/commit/cba0096c1455f5e697756791441679f52d10c3b9))
* cannot seek subscription in consumer with multiple workers ([3f18fb8](https://github.com/efcasado/pulsar-elixir/commit/3f18fb8b9d0bfafdd812e1de56d082ce6d53aa44))
* cannot set start_delay_ms using application env ([b6e48af](https://github.com/efcasado/pulsar-elixir/commit/b6e48afdc78846baaba5ad1aac9f1c952a4c387a))
* concurrent named consumers not starting due to name clash ([88421fc](https://github.com/efcasado/pulsar-elixir/commit/88421fc17e54d89007f16d38faf51046897fb3aa))
* configurable start up delay ([#32](https://github.com/efcasado/pulsar-elixir/issues/32)) ([1bc5c7b](https://github.com/efcasado/pulsar-elixir/commit/1bc5c7bee3acf7c71f8ccc3d43368f8b6a5e4431))
* do not ignore broker opts from config ([cd49e1d](https://github.com/efcasado/pulsar-elixir/commit/cd49e1d24865ae3f8528ff95973eec42a0508cf2))
* do not start consumers if cannot subscribe to topic ([b1b5e73](https://github.com/efcasado/pulsar-elixir/commit/b1b5e73ac3342b5d7f5769577f0096f43fb3854b))
* flow control issues when consuming batched messages ([527fe98](https://github.com/efcasado/pulsar-elixir/commit/527fe98eadbe95a06541c8e9d65f3de02d8d90e9))
* force-reply to pending requests ([c2419f2](https://github.com/efcasado/pulsar-elixir/commit/c2419f2f5e0245f4715ff0e9dca6db6d81a455aa))
* handle incomplete messages from the broker ([f4789a8](https://github.com/efcasado/pulsar-elixir/commit/f4789a879d816e73bb202d88f3d56dff4b3c732c))
* message not uncompressed when missing broker metadata ([6aba8e3](https://github.com/efcasado/pulsar-elixir/commit/6aba8e3bab143c1c229d0b2f50c8cfe6ede13bbe))
* nested opts not passed to consumers/producers ([5050df1](https://github.com/efcasado/pulsar-elixir/commit/5050df1b63127de63311c75b487f75607febeca2))
* OAuth2 settings examples in README ([#51](https://github.com/efcasado/pulsar-elixir/issues/51)) ([963e55c](https://github.com/efcasado/pulsar-elixir/commit/963e55c96f28f1d160c2519410991602396f5314))
* overloaded broker during startup ([dbb4bc6](https://github.com/efcasado/pulsar-elixir/commit/dbb4bc6ab556eb069dfcf4cb1d153b427f365c3f))
* parallelize processing of messages ([#49](https://github.com/efcasado/pulsar-elixir/issues/49)) ([ed695b1](https://github.com/efcasado/pulsar-elixir/commit/ed695b1b95fa55f29c1b095e48c73dfabd76cc7a))
* producers cannot be start using Pulsar.start/1 ([40ef2bb](https://github.com/efcasado/pulsar-elixir/commit/40ef2bb167036d9d8e79836af5e2115603501d8f))
* rework consumer/producer startup logic ([6d0e464](https://github.com/efcasado/pulsar-elixir/commit/6d0e4646e84dbb4d3a22ed04863c605d08c1ed37))
* safer default for secure tls connections ([#33](https://github.com/efcasado/pulsar-elixir/issues/33)) ([5563aac](https://github.com/efcasado/pulsar-elixir/commit/5563aac858a372bfe31d70f5bffaaf6f02c2e0f0))
* slow broker reconnects ([806b295](https://github.com/efcasado/pulsar-elixir/commit/806b29561a615b1625db071543fbde29c2de643f))
* verbose logs when consumer in crash loop ([f25bc2e](https://github.com/efcasado/pulsar-elixir/commit/f25bc2eac0b8d39aa3a604d30525bcaa0c0de38f))
* zombie consumer due to race condition ([5677dd0](https://github.com/efcasado/pulsar-elixir/commit/5677dd08ec8f1e7da0f81cb4bd3daa76949d4703))

### 🛠 Builds

* re-organize consumer tests to speed-up builds ([#35](https://github.com/efcasado/pulsar-elixir/issues/35)) ([3887fa3](https://github.com/efcasado/pulsar-elixir/commit/3887fa32c0ef79e2f7da2a7c0b7118c157cf95ca))
* start/stop pulsar cluster only once per build ([3dc4b23](https://github.com/efcasado/pulsar-elixir/commit/3dc4b23daf009aeb3ea82b281f24ce6b73e2f07f))

### 📦 Code Refactoring

* client architecture ([#58](https://github.com/efcasado/pulsar-elixir/issues/58)) ([f1d501d](https://github.com/efcasado/pulsar-elixir/commit/f1d501df0b2783211d620ccc2df4e14d72556ed6))
* move topic lookup logic to service discovery module ([ef7028c](https://github.com/efcasado/pulsar-elixir/commit/ef7028cce4cc4672631c6e9690124939e7a7f281))

### ⚙️ Continuous Integrations

* automated releases ([6f6415d](https://github.com/efcasado/pulsar-elixir/commit/6f6415dc8c54d88ee1d0a9b75c4777d229ddaea5))
* automated releases ([9d2cce1](https://github.com/efcasado/pulsar-elixir/commit/9d2cce1879505efee05d121931b86f9f79d5b20c))
* bump insurgent/conventional-changelog-preset version [skip ci] ([b36ffae](https://github.com/efcasado/pulsar-elixir/commit/b36ffae02c2df570326b00dac369b89f0e5a750e))
* set up initial gha workflow ([a478891](https://github.com/efcasado/pulsar-elixir/commit/a47889156aff94c5cf9b85929a36a59623f8a3af))

### ♻️ Chores

* add 'features' section to readme ([11c29f7](https://github.com/efcasado/pulsar-elixir/commit/11c29f797ce7225caf84bfe1592cd31c605bbfed))
* add 'testing' section to readme ([fb66a04](https://github.com/efcasado/pulsar-elixir/commit/fb66a0425bd919df37c040e0143cdda4422ffbf3))
* bump pulsar version ([46eca44](https://github.com/efcasado/pulsar-elixir/commit/46eca442fe6996bb8f3c266ab0aeaa2b7ee31f09))
* **ci:** run only one build per branch ([ef9eb54](https://github.com/efcasado/pulsar-elixir/commit/ef9eb546c0f07d8fbe16c18a30dd69c205610c33))
* end-to-end example ([f65ea58](https://github.com/efcasado/pulsar-elixir/commit/f65ea58c2e9e80de32a342abd2280ab20c425063))
* format codebase using styler ([32179f0](https://github.com/efcasado/pulsar-elixir/commit/32179f092a07c9b3a146f40c67a2549f431a521e))
* run mix format ([cb8edd7](https://github.com/efcasado/pulsar-elixir/commit/cb8edd719390f2044fc0aee77a9fc077e1d1ebeb))
* set log level to info when running tests ([fa0948a](https://github.com/efcasado/pulsar-elixir/commit/fa0948aba41c584a3f57e5804da63280e1f595cb))
* update readme ([5a37af0](https://github.com/efcasado/pulsar-elixir/commit/5a37af083fb937644a42bbcfaf62c486972540e8))
* update readme ([027cae0](https://github.com/efcasado/pulsar-elixir/commit/027cae00760b93f33a7607e4329580271801937d))
* update readme ([75f787e](https://github.com/efcasado/pulsar-elixir/commit/75f787edc61aba48016323fd3e76338446b53b7f))
* update README.md ([74d46ec](https://github.com/efcasado/pulsar-elixir/commit/74d46ec5ea05b821c3f18645d3b086aa6def596b))
* use mise to configure development environment ([76d7b5a](https://github.com/efcasado/pulsar-elixir/commit/76d7b5affb8a515485f71bc12bd69bc0710f736b))
