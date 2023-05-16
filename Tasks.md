# Tasks related to fuzzing redisraft fuzzing

- [ ] Integrate with Netrix
    - [ ] Build netrix communication library
        - [ ] Create config parameter to initialize interception
    - [ ] Intercept message sends
        - [ ] Modularize serialization and deserialization
    - [ ] Invoke callbacks on replies
    - [ ] Capturing non deterministic choices
        - [ ] Random timeouts
- [ ] Fuzzing 