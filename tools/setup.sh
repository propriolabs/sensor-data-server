sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer

wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.3/elasticsearch-2.3.3.tar.gz 
tar -xvf elasticsearch-2.3.3.tar.gz
rm elasticsearch-2.3.3.tar.gz

sudo apt-get install nodejs
sudo apt-get install npm
sudo ln -s /usr/bin/nodejs /usr/bin/node

sudo apt-get install git
mkdir proprio
cd proprio
git clone https://github.com/propriolabs/data-server
cd data-server
npm install

sudo ./fix_es.sh
