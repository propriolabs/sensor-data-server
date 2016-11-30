(crontab -l 2>/dev/null; echo "46 * * * * /home/ubuntu/proprio/production/data-server/run_userupdates.sh") | crontab -
(crontab -l 2>/dev/null; echo "17 * * * * /home/ubuntu/proprio/production/data-server/run_ranks.sh") | crontab -
