#!/bin/bash

# Spark Installation Script for Ubuntu VM
# This script installs Java, Spark, and sets up Jupyter notebook access

set -e  # Exit on any error

echo "=== Spark Installation Script for Ubuntu VM ==="
echo "This script will install Java, Spark, and Jupyter for notebook access"
echo

# Update system packages
echo "1. Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Java 8 (required for Spark)
echo "2. Installing Java 8..."
sudo apt install -y openjdk-8-jdk openjdk-8-jre

# Set JAVA_HOME
echo "3. Setting up Java environment..."
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc

# Install Python 3.11 (not available by default)
echo "4. Installing Python 3.11..."
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-distutils python3.11-dev

# Make python3.11 the default python3 (optional, but recommended for this script)
echo "5. Setting Python 3.11 as default python3..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
sudo update-alternatives --set python3 /usr/bin/python3.11

# Create a virtual environment for Spark
echo "6. Creating Python virtual environment..."
python3 -m venv ~/spark-env
source ~/spark-env/bin/activate

# Install required Python packages
echo "7. Installing Python packages..."
pip install --upgrade pip
pip install jupyter pyspark ipykernel findspark pandas matplotlib seaborn numpy pyarrow
python -m ipykernel install --user --name spark-env --display-name "Python (spark-env)"

# Download and install Spark
echo "8. Downloading and installing Apache Spark..."
SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
SPARK_DIR="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_TGZ="${SPARK_DIR}.tgz"

cd /tmp
wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
tar -xzf "${SPARK_TGZ}"
sudo mv "${SPARK_DIR}" /opt/spark
sudo chown -R $USER:$USER /opt/spark

# Set up Spark environment variables
echo "9. Setting up Spark environment variables..."
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3

# Add environment variables to bashrc
cat >> ~/.bashrc << 'EOF'

# Spark Environment Variables
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3

source ~/spark-env/bin/activate
EOF

# Create Jupyter configuration
echo "10. Setting up Jupyter notebook configuration..."
mkdir -p ~/.jupyter
cat > ~/.jupyter/jupyter_notebook_config.py << 'EOF'
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.allow_root = True
c.NotebookApp.token = ''
c.NotebookApp.password = ''
EOF

# Create a sample Spark notebook
echo "11. Creating sample Spark notebook..."
mkdir -p ~/spark-notebooks
cat > ~/spark-notebooks/spark_test.ipynb << 'EOF'
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Import required libraries\n",
    "import findspark\n",
    "findspark.init()\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark import SparkContext, SparkConf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create Spark session\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"SparkTest\") \\\n",
    "    .master(\"local[*]\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "print(f\"Spark version: {spark.version}\")\n",
    "print(f\"Spark context: {spark.sparkContext}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Test with sample data\n",
    "data = [(\"Alice\", 25), (\"Bob\", 30), (\"Charlie\", 35)]\n",
    "columns = [\"Name\", \"Age\"]\n",
    "\n",
    "df = spark.createDataFrame(data, columns)\n",
    "df.show()\n",
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Stop Spark session\n",
    "spark.stop()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
EOF

# Create startup script
echo "12. Creating startup scripts..."
cat > ~/start-spark-notebook.sh << 'EOF'
#!/bin/bash
# Script to start Jupyter notebook with Spark in background

# Activate virtual environment
source ~/spark-env/bin/activate

# Set environment variables
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3

# Create logs directory
mkdir -p ~/spark-logs

# Start Jupyter notebook in background
cd ~/spark-notebooks
nohup jupyter notebook --no-browser --ip=0.0.0.0 --port=8888 --allow-root > ~/spark-logs/jupyter.log 2>&1 &

echo "Jupyter notebook started in background"
echo "Access at: http://$(hostname -I | awk '{print $1}'):8888"
echo "Log file: ~/spark-logs/jupyter.log"
echo "To stop: pkill -f jupyter"
EOF

chmod +x ~/start-spark-notebook.sh

# Create standalone Spark start script
cat > ~/start-spark-standalone.sh << 'EOF'
#!/bin/bash
# Script to start Spark in standalone mode

# Set environment variables
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Start Spark master
$SPARK_HOME/sbin/start-master.sh

# Start Spark worker
$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077

echo "Spark cluster started!"
echo "Master Web UI: http://$(hostname -I | awk '{print $1}'):8080"
echo "Master URL: spark://$(hostname):7077"
EOF

chmod +x ~/start-spark-standalone.sh

# Create stop script
cat > ~/stop-spark.sh << 'EOF'
#!/bin/bash
# Script to stop Spark cluster and Jupyter

export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Stop Spark services
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh

# Stop Jupyter notebook
pkill -f jupyter

echo "Spark cluster and Jupyter stopped!"
EOF

chmod +x ~/stop-spark.sh

# Add a status check script
cat > ~/spark-status.sh << 'EOF'
#!/bin/bash
# Check status of Spark services

echo "=== Spark Services Status ==="
echo

# Check Spark master
if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
    echo "✅ Spark Master: Running"
else
    echo "❌ Spark Master: Not running"
fi

# Check Spark worker
if pgrep -f "org.apache.spark.deploy.worker.Worker" > /dev/null; then
    echo "✅ Spark Worker: Running"
else
    echo "❌ Spark Worker: Not running"
fi

# Check Jupyter
if pgrep -f jupyter > /dev/null; then
    echo "✅ Jupyter Notebook: Running"
else
    echo "❌ Jupyter Notebook: Not running"
fi

echo
echo "Spark Master Pivate Web UI: http://$(hostname -I | awk '{print $1}'):8080"
echo "Private Jupyter Notebook: http://$(hostname -I | awk '{print $1}'):8888"

echo
echo "Spark Master Public Web UI: http://$(curl -s icanhazip.com):8080"
echo "Public Jupyter Notebook: http://$(curl -s icanhazip.com):8888"
EOF

chmod +x ~/spark-status.sh

# Test Spark installation
echo "13. Testing Spark installation..."
source ~/.bashrc
source ~/spark-env/bin/activate

python3 -c "
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Test').master('local[*]').getOrCreate()
print('Spark version:', spark.version)
spark.stop()
print('Spark test successful!')
"

echo
echo "=== Installation Complete! ==="
echo
echo "Spark has been successfully installed!"
echo
echo "Usage Commands:"
echo "# Start services (runs in background)"
echo "~/start-spark-notebook.sh"
echo "~/start-spark-standalone.sh"
echo
echo "# Check status"
echo "~/spark-status.sh"
echo
echo "# Stop all services"
echo "~/stop-spark.sh"
echo
echo "To access Spark via Jupyter notebook:"
echo "1. Run: ~/start-spark-notebook.sh"
echo "2. Open browser: http://YOUR_VM_IP:8888"
echo "3. Open the sample notebook: spark_test.ipynb"
echo
echo "To start Spark standalone cluster:"
echo "1. Run: ~/start-spark-standalone.sh"
echo "2. Access Web UI: http://YOUR_VM_IP:8080"
echo
echo "Environment setup:"
echo "- Virtual environment: ~/spark-env"
echo "- Spark installation: /opt/spark"
echo "- Notebooks directory: ~/spark-notebooks"
echo
echo "Manual activation (if needed):"
echo "source ~/spark-env/bin/activate"
echo "source ~/.bashrc"
echo
echo "Next steps:"
echo "1. Configure firewall to allow port 8888 and 8080"
echo "2. Test with: ~/start-spark-notebook.sh"
echo "3. Access from browser using your VM's IP address"


# 1. Save the script:
## Paste the script content
#chmod +x install-spark.sh

# 2. Run the installation:
#./install-spark.sh

# 3. Start Jupyter with Spark:
#~/start-spark-notebook.sh

# 4. Access from your browser:
#http://YOUR_VM_IP:8888

#sudo mv /usr/lib/cnf-update-db /usr/lib/cnf-update-db.disabled
#sudo apt-get update
#sudo mv /usr/lib/cnf-update-db.disabled /usr/lib/cnf-update-db

