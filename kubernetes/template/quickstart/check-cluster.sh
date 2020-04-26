## Check status
kubectl get statefulset -n kylin-quickstart
kubectl get pod -n kylin-quickstart --output=yaml
kubectl get service -n kylin-quickstart --output=yaml

## Check detail
kubectl describe statefulset kylin-all -n kylin-quickstart