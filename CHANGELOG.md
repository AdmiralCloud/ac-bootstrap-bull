<a name="1.0.4"></a>

## [1.0.4](https://github.com/admiralcloud/ac-bootstrap-bull/compare/v1.0.3..v1.0.4) (2021-12-12 14:04:52)


### Bug Fix

* **App:** Typo fix - processing with 2 s only | MP | [856d47dc8018f03a4aa2ac8aaade68239e2cd6b7](https://github.com/admiralcloud/ac-bootstrap-bull/commit/856d47dc8018f03a4aa2ac8aaade68239e2cd6b7)    
Rename postProcesssing (3s) to postProcessing (2s)
### Chores

* **App:** Updated packages | MP | [d6ca1ef636269aee01b3a15c74f589e317a6f1c9](https://github.com/admiralcloud/ac-bootstrap-bull/commit/d6ca1ef636269aee01b3a15c74f589e317a6f1c9)    
Updated packages
<a name="1.0.3"></a>

## [1.0.3](https://github.com/admiralcloud/ac-bootstrap-bull/compare/v1.0.2..v1.0.3) (2021-11-27 07:46:38)


### Bug Fix

* **App:** Fixed Redis key, update Redis config, improved function naming | MP | [eafc58717551dc494bb1eae690c7081b44314487](https://github.com/admiralcloud/ac-bootstrap-bull/commit/eafc58717551dc494bb1eae690c7081b44314487)    
Prepare processing is now postProcessing in order to avoid confusion. Redis config is now prepared for Bull 4 (breaking change in Bull), Fixed Redis key
### Chores

* **App:** Updated packages | MP | [f55e9cd6e0f984fbeb78eaf35fcbb5604c6c5687](https://github.com/admiralcloud/ac-bootstrap-bull/commit/f55e9cd6e0f984fbeb78eaf35fcbb5604c6c5687)    
Updated packages
<a name="1.0.2"></a>

## [1.0.2](https://github.com/admiralcloud/ac-bootstrap-bull/compare/v1.0.1..v1.0.2) (2021-10-15 09:45:36)


### Bug Fix

* **App:** Make logLevel for redislock configurable | MP | [247c64dd648ea4086ac6c4d8e7abe7f4cf08979a](https://github.com/admiralcloud/ac-bootstrap-bull/commit/247c64dd648ea4086ac6c4d8e7abe7f4cf08979a)    
Make logLevel for redislock configurable and fallback to silly
<a name="1.0.1"></a>

## [1.0.1](https://github.com/admiralcloud/ac-bootstrap-bull/compare/v1.0.0..v1.0.1) (2021-10-14 16:09:42)


### Bug Fix

* **App:** Fixed init redisLock | MP | [921cff9e1a72f8ff652417a720e6e3db19719e89](https://github.com/admiralcloud/ac-bootstrap-bull/commit/921cff9e1a72f8ff652417a720e6e3db19719e89)    
Fixed init redisLock
<a name="1.0.0"></a>
 
# [1.0.0](https://github.com/admiralcloud/ac-bootstrap-bull/compare/v0.0.1..v1.0.0) (2021-10-14 15:28:35)


### Feature

* **App:** Force release of version 1 | MP | [d4b375487909d8ebc4833c2093570c425b11557a](https://github.com/admiralcloud/ac-bootstrap-bull/commit/d4b375487909d8ebc4833c2093570c425b11557a)    
Force release of version 1
### Bug Fix

* **App:** Do not reuse RedisLock | MP | [f6a3a66f119606cf377e4b08c08a5c53a9207df0](https://github.com/admiralcloud/ac-bootstrap-bull/commit/f6a3a66f119606cf377e4b08c08a5c53a9207df0)    
RedisLock must be initiated otherwise it will not work as we cannot reuse/share the redislock from parent application
### Chores

* **App:** Updated packages | MP | [7861eaa49e8cab64d2d4b13395c30e8f1cb1e170](https://github.com/admiralcloud/ac-bootstrap-bull/commit/7861eaa49e8cab64d2d4b13395c30e8f1cb1e170)    
Updated packages
## BREAKING CHANGES
* **App:** Force release of version 1
<a name="0.0.1"></a>

## [0.0.1](https://github.com/admiralcloud/ac-bootstrap-bull/compare/..v0.0.1) (2021-10-09 10:54:16)


### Bug Fix

* **App:** Moved to seperate repo | MP | [274b17945bb72d0798fecf877a8ca558535919e3](https://github.com/admiralcloud/ac-bootstrap-bull/commit/274b17945bb72d0798fecf877a8ca558535919e3)    
No longer mono-repo
undefined