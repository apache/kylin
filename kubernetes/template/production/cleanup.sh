kubectl delete -f deployment/memcached/memcached-service.yaml
kubectl delete -f deployment/memcached/memcached-statefulset.yaml

kubectl delete -f deployment/kylin/kylin-service.yaml
kubectl delete -f deployment/kylin/kylin-all-statefulset.yaml
kubectl delete -f deployment/kylin/kylin-job-statefulset.yaml