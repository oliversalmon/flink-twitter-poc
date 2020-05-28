Install Flink on K8 using Helm

See https://hub.helm.sh/charts/riskfocus/flink

`helm repo add riskfocus https://riskfocus.github.io/helm-charts-public`

`helm install --kubeconfig=flink-mongo-cluster-config.yaml  my-flink riskfocus/flink`
 
 Note: assumes the k8 cluster in DO is called flink-mongo-cluster and you have kubeconfig file in the current directory to run this command. If not,download from DO 