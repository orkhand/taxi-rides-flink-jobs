# autoscaler-metrics-decoder

Flink K8S AUtoscaler stores collected metrics and decision in configmap in K8S.

But the data is first Gzip compressed and then base64 encoded.

To convert it into human readible format we need to perform 2 actions:
1. Decode each line of configmap with base64
2. Decompress each decoded line with Gzip compression.