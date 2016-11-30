# Proprio-Data-Server
Proprio Labs' server to handle incoming data requests from mobile phones and smartwatches

## Set up

First, you'll need to edit the config.js and config.py files. The IP and port for this server must be configured correctly in Proprio-Android-Wear. You also must change the analysis_server value to point to the Proprio-Motion-Classification server.

		npm install
		node serve.js

