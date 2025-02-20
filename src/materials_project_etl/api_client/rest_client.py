from mp_api.client import MPRester
from mp_api.client.mprester import Session


class RestClient:
    def __init__(self,
                 api_key: str, ):
        if api_key is None or len(api_key) == 0:
            raise EnvironmentError("No API key provided for Materials Project API")
        self.api_key = api_key
        self.session = Session()

    @property
    def mp_rester(self):
        return MPRester(self.api_key, session=self.session)
