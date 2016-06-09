if [ -z "$SEEDS" ]; then
    echo "No seeds specified, being my own seed..."
	SEEDS = "[\":7001\"]"
fi
sed -i -e "s/BootstrapNodes: \[\":7001\"\]/BootstrapNodes: $SEEDS/" /go/bin/config.yml

/go/bin/swimring