> Please check README under `hadoop-client` and `kylin-client` for detail.

After kylin-client image is ready, you can use `docker` command to save or push the image.

```shell
# Save and share it with others.
docker save -o kylin-cdh.tar kylin-client:3.0.1-cdh57
# Or push it to registry
docker push apachekylin/kylin-client:3.0.1-cdh57
```