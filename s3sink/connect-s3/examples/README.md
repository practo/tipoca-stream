# README

```
kafka -> s3connector -> my-connect-cluster -> s3
```

### Install
```
k create -f connector-configmap.yaml
k create -f kc-my-connect-cluster.yaml
k create -f kctr-s3-sink-connector.yaml
```

### Test


- Trigger change:
```
```

### Cleanup
```
```
