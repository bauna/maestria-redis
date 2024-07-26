import pytest
import no_sql.main as main


class TestRedis:
  @pytest.mark.asyncio
  async def test_set(self, redis_client):
    key = 'key'
    value = 'a_value'
    await main.redis_set(key, value)
    assert redis_client.get(key) == value.encode()
