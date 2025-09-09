for i in {1..10}; do
  curl -s -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{
      "transaction_id": "load-test-'$i'",
      "customer_id": 123,
      "terminal_id": 456,
      "tx_amount": 100.0,
      "tx_time_seconds": 1693737600,
      "tx_time_days": 19723,
      "tx_fraud_scenario": 0
    }' && echo " ✅ Request $i completed"
  sleep 1
done