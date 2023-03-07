(cd ../Manager && ./start.sh &)
sleep 5
(cd ../Manager && export PORT=8002 && ./start.sh -r &)
(cd ../Manager && export PORT=8003 && ./start.sh -r &)
sleep 5
(cd ../Broker && export PORT=8004 && ./start.sh &)
(cd ../Broker && export PORT=8005 && ./start.sh &)
(cd ../Broker && export PORT=8006 && ./start.sh &)
(cd ../Broker && export PORT=8007 && ./start.sh &)
(cd ../Broker && export PORT=8008 && ./start.sh &)