from opinion_clob_sdk import Client
from opinion_clob_sdk.model import TopicType

client = Client(
    host="https://proxy.opinion.trade:8443",
    apikey="9dTINwB9JRjRbxCb1y9Asxw1GrOGDpQl",
    chain_id=56,
    rpc_url="https://bsc-dataseed.binance.org",
    private_key="0xab088bc74806e4a70f5edd271c45afbab97c04dba4dd8b9b2dc26770b2a62cbd",
    multi_sig_addr="0xd05ff050cd8fc040e2e74c6321728725e2cbf551",
)

print(client.get_markets(topic_type=TopicType.ALL, page=1, limit=10))
print(client.get_my_balances())