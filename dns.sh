#!/bin/bash

echo =========== Create an ubuntu pod ==================
kubectl run utils --image=arunvelsriram/utils -- bash -c "while true; do echo hello; sleep 10;done"

# Wait for the pod "ubuntu" to contain the status condition of type "Ready"
kubectl wait --for=condition=Ready pod/utils

# Save a sorted list of IPs of all of the k8s SVCs:
kubectl get svc -A|egrep -v 'CLUSTER-IP|None'|awk '{print $4}'|sort -V > ips

# Copy the ip list to owr Ubuntu pod:
kubectl cp ips utils:/

# Print 7 blank lines
yes '' | sed 7q
echo =========== Print all k8s SVC DNS records ====================
for ip in $(cat ips); do echo -n "$ip "; kubectl exec -it utils -- dig -x $ip +short; done
echo ====== End of list =====================

echo ========= Cleanup  ===============
kubectl delete po utils
rm ips
exit 0