import grpc
from concurrent import futures
import ipaddress
import subprocess
import asyncio
import os
import json
from grpc_dir import scraping_pb2, scraping_pb2_grpc

# health
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_health.v1 import health as health_srv

from scraping import 정부기관NEWS_Scraping

GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
IS_DOCKER = os.getenv("IS_DOCKER", "false").lower() == "true"

DB_ATTRIBUTES = os.getenv("DB_ATTRIBUTES", "제목,등록일,링크").split(",")
TH_DB = json.loads(os.getenv("TH_DB", '{"제목":"제목","등록일":"등록일","작성일":"등록일","등록일자":"등록일","날짜":"등록일","장학생":"제목금지어","박종선":"제목금지어"}'))



class GovNewsScraperServicer(scraping_pb2_grpc.GovNewsScraperServicer):
    def GetNews(self, request, context):
        config = {
            "url": request.url,
            "gov_name": request.gov_name,
            "구분": request.category,
            "suffix_link": request.suffix_link,
        }

        scraper = 정부기관NEWS_Scraping(config=config, th_db=TH_DB, db_attributes=DB_ATTRIBUTES)
        items = [
            scraping_pb2.NewsItem(
                gov_name=i.get("정부기관", ""),
                category=i.get("구분", ""),
                title=i.get("제목", ""),
                date=str(i.get("등록일", "")),
                link=i.get("링크", ""),
            )
            for i in scraper.results
        ]
        return scraping_pb2.ScrapeResponse(items=items, errors=scraper.errorList)

async def serve():
    # non-async health server implementation provided by grpcio-health-checking
    health_manager = health_srv.HealthServicer()
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))

    # register services
    scraping_pb2_grpc.add_GovNewsScraperServicer_to_server(GovNewsScraperServicer(), server)
    health_pb2_grpc.add_HealthServicer_to_server(health_manager, server)

    # set overall serving status to NOT_SERVING until ready
    health_manager.set('', health_pb2.HealthCheckResponse.NOT_SERVING)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await server.start()
    # once server started, mark as SERVING
    health_manager.set('', health_pb2.HealthCheckResponse.SERVING)
    print(f"PingService running on port {GRPC_PORT}")

    try:
        await server.wait_for_termination()
    finally:
        # on shutdown mark NOT_SERVING
        health_manager.set('', health_pb2.HealthCheckResponse.NOT_SERVING)
        await server.stop(5)

if __name__ == "__main__":
    print("ENV")
    print("--------------------------------")
    print(f"IS_DOCKER: {IS_DOCKER}")
    print(f"GRPC_PORT: {GRPC_PORT}")
    print(f"MAX_WORKERS: {MAX_WORKERS}")
    print(f"DB_ATTRIBUTES: {DB_ATTRIBUTES}")
    print(f"TH_DB: {TH_DB}")
    print("--------------------------------")
    asyncio.run(serve())