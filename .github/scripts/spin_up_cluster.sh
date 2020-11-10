#!/bin/bash

echo "Initializing AWS cluster..."
timeout --foreground 120 tlp-cluster init CASS CASSANDRA-15580 "4.0 repair quality testing" -c 3 --instance m5ad.xlarge > init.log 2>&1
echo "Spinning up EC2 instances..."
timeout --foreground 120 tlp-cluster up --auto-approve > up.log 2>&1
# tlp-cluster build isn't working anymore. We have to use existing packages until we fix this.
timeout --foreground 180 tlp-cluster use 4.0~alpha4 --config "cluster_name:repair_quality" > use.log 2>&1
# Allow external JMX access
sed -i '1s/^/LOCAL_JMX=no\n/' provisioning/cassandra/conf/cassandra-env.sh
sed -i 's/com.sun.management.jmxremote.authenticate=true/com.sun.management.jmxremote.authenticate=false/' provisioning/cassandra/conf/cassandra-env.sh
echo "Installing packages..."
timeout --foreground 600 tlp-cluster install > install.log 2>&1 || echo "meh... install phase seem to have failed"
# after configuring instances, but before starting them, patch cassandra-env to open jmx on public IPs
shopt -s expand_aliases || setopt aliases
source env.sh
c_all grep hostname /etc/cassandra/cassandra-env.sh
echo "Starting Cassandra..."
timeout --foreground 600 tlp-cluster start > start.log 2>&1 || echo "meh... start phase seem to have failed"
# Push the rtest on the monitoring node to run them from there
tar czf cassandra-rtest.tar.gz *
scp cassandra-rtest.tar.gz monitoring0:/home/ubuntu
ssh monitoring0 "mkdir -p /home/ubuntu/.tlp-cluster/profiles/defaults/"
scp ~/.tlp-cluster/profiles/default/secret.pem monitoring0:/home/ubuntu/.tlp-cluster/profiles/default/
cat << EOF >> run_test_suite.sh
set -x
sudo apt-get install jq -y
curl -sL https://github.com/shyiko/jabba/raw/master/install.sh | bash && . ~/.jabba/jabba.sh
. ~/.jabba/jabba.sh && jabba install adopt@1.8.0-272
. ~/.jabba/jabba.sh && jabba alias default adopt@1.8.0-272
. ~/.jabba/jabba.sh && sudo update-alternatives --install /usr/bin/java java ${JAVA_HOME%*/}/bin/java 20000
. ~/.jabba/jabba.sh && sudo update-alternatives --install /usr/bin/javac javac ${JAVA_HOME%*/}/bin/javac 20000
sudo apt-get install maven -y
i=0
for ip in \$(cat provisioning/monitoring/config/prometheus/tg_mcac.json | jq -r '.[]?.targets[0]'|cut -d':' -f1); do export CLUSTER_CONTACT_POINT\$i=\$ip; ((i=i+1)); done
tar xvf /home/ubuntu/cassandra-rtest.tar.gz
set -e
CLUSTER_KIND=aws mvn test -Dcucumber.filter.tags="@Full"
CLUSTER_KIND=aws mvn test -Dcucumber.filter.tags="@Incremental"
EOF

scp run_test_suite.sh monitoring0:/home/ubuntu
ssh monitoring0 "chmod u+x /home/ubuntu/run_test_suite.sh"
ssh monitoring0 "./run_test_suite.sh"