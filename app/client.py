import grpc
import os, time
from grpc_dir import scraping_pb2, scraping_pb2_grpc
# health
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_health.v1 import health as health_srv

# GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))
# CHANNEL = f"localhost:{GRPC_PORT}"
GRPC_PORT = int(os.getenv("GRPC_PORT", 5555))
CHANNEL = f"scraping-gov.grpc.sh:{GRPC_PORT}"

def grpc_health_check(target:str, timeout:int=2) -> bool:
  try:
      with grpc.insecure_channel(target) as ch:
          stub = health_pb2_grpc.HealthStub(ch)
          req = health_pb2.HealthCheckRequest(service="")
          resp = stub.Check(req, timeout=timeout)
          return resp.status == health_pb2.HealthCheckResponse.SERVING
  except Exception as e:
      print(f"[grpc_health_check] {e}")
      return False

def run():
    with grpc.insecure_channel(CHANNEL) as channel:
        stub = scraping_pb2_grpc.GovNewsScraperStub(channel)
        req = scraping_pb2.ScrapeRequest(
            url="http://www.molit.go.kr/USR/NEWS/m_71/lst.jsp  ".strip(),
            gov_name="국토교통부  ".strip(),
            category="보도자료  ".strip(),
            suffix_link="https://www.molit.go.kr/USR/NEWS/m_71/".strip()
        )
        res = stub.GetNews(req)
        for item in res.items:
            print(item.gov_name, item.category, item.title, item.date, item.link)
        if res.errors:
            print("Errors:", res.errors)

if __name__ == "__main__":
    start_time = time.perf_counter()
    print( "grpc_health_check: ", CHANNEL, ':', grpc_health_check(target = CHANNEL))
    run()
    end_time = time.perf_counter()
    print(f"Time taken: {int( 1000*(end_time - start_time) )} milliseconds")