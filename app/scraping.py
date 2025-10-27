from bs4 import BeautifulSoup
import concurrent.futures
import requests
from urllib.parse import urlparse
from urllib.request import urlopen
import datetime
import time, json, copy
from dateutil import parser

HEADERS = {
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
	"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
	"Connection": "keep-alive"
}


class 정부기관NEWS_Scraping:
	""" 정부기관 뉴스 스크래핑 
		config :  dict :{'url': 'https://www.moel.go.kr/news/enews/report/enewsList.do', 
						'gov_name': '고용노동부', 
						'구분': '보도자료', 
						'suffix_link': 'https://www.moel.go.kr/news/enews/report/'}
		th_db :  dict  : {'제목': '제목', '등록일': '등록일', '작성일': '등록일', 
					'등록일자': '등록일', '날짜': '등록일', 
					'장학생': '제목금지어', '박종선': '제목금지어'}
	"""
	def __init__(self, config:dict={}, th_db:dict={}, db_attributes:list[str]=[]):
		self.gov_name: str = ''
		self.구분: str = ''
		self.url: str = ''
		self.link_suffix: str = ''
		self.db_attributes: list[str] = db_attributes
		self.ths, self.tds, self.results = [], [], []

		self.th_db = th_db
		self.config = config
		for key, value in self.config.items():
			setattr(self, key, value)

		self.link_testResult = False

		self.defaultDict = {
			'정부기관':self.gov_name,
			'구분' : self.구분,
		}
		self.result = {}
		self.log = {}
		self.errorList = []
		self.timeout = 5.0
		self.soup = self.get_soup( self.url )
		self.run()

	def run(self) -> list[dict]:
		if self.soup is not None:
			match self.gov_name:
				# case '한국소방안전원':		
				# 	ul = self.soup.find( 'ul', {'class':'orderlist'})
				# 	lis = ul.find_all('li')
				# 	for (index,li) in enumerate(lis):
				# 		li : BeautifulSoup
				# 		if ( title:= li.find('p', {'class':'title'}) ):
				# 			title_text = title.text.strip().replace( title.find('span').text, "") 
				# 			_isValid, 등록일 = self.check_등록일_검증(등록일 = li.find_all('span')[2].text.strip() )
				# 			if _isValid :
				# 				link = self.link_suffix + self.get_href( a = li.find('a', href=True) )
				# 				updateDict = copy.deepcopy( self.defaultDict )
				# 				updateDict.update(
				# 					{
				# 						'제목':title_text,
				# 						'등록일' : 등록일,
				# 						'링크' : link,
				# 					} 
				# 				)
				# 				self.results.append (updateDict)

				# 				if index == 0:
				# 					self.check_link_test(link)							

				case _:
					self.ths:list[str] = self.get_tableThs()
					result_list = self.get_tableTds()
					self.results = self.validate_result(result_list)

		return self.results

	# 문자열을 datetime 객체로 파싱한 후 date() 메서드로 날짜만 추출
	def parse_to_date(self, date_string:str) -> datetime.date|None:
		try:
			# 문자열을 datetime 객체로 파싱
			dt = parser.parse(date_string)
			# datetime에서 date 객체만 추출
			return dt.date()
		except Exception as e:
			print(f"날짜 파싱 오류: {e}")
			return None
		
	def validate_result(self, result_list:list[dict]) -> list[dict]:
		""" 검증 결과 반환 """

		날짜_str = None
		for _th_str in self.ths:
			match self.th_db.get(_th_str, None):
				case '제목':
					pass
				case '등록일':
					날짜_str = _th_str
				case _:
					pass

		results = []
		for _obj in result_list:
			# 새 딕셔너리를 생성하여 기존 딕셔너리를 복사
			new_obj = copy.deepcopy(self.defaultDict)
			
			for key, value in _obj.items():
				if key in self.db_attributes:
					if key == 날짜_str:
						new_obj['등록일'] = self.parse_to_date(value)
					else:
						new_obj[key] = value
					
			results.append(new_obj)
		return results

	
	def check_link_test(self, link:str) -> bool:
		### 😀 https://stackoverflow.com/questions/56101612/python-requests-http-response-406
		res = requests.get( link, timeout= self.timeout, headers={"User-Agent": "XY"})
		self.link_testResult = res.ok
		if not self.link_testResult:
			self.errorList.append(f"  {link} test Falil : {res.status_code} -- {self.defaultDict['정부기관']} - {self.defaultDict['구분']}")
		return res.ok
	

	def get_soup(self, url) -> BeautifulSoup|None :
		if not url : return None
		try:
			# requests는 자동으로 30x follow
			res = requests.get(url, headers=HEADERS, timeout=self.timeout)
			res.raise_for_status()
			return BeautifulSoup(res.text, "html.parser")
			# page = urlopen(url, timeout= self.timeout)
			# html:str = page.read().decode("utf-8")
			# soup = BeautifulSoup(html, features="html.parser")
			# return soup
		except Exception as e:
			print(f"get_soup error: {e}")
			self.errorList.append(str(e))
			self.log['url'] = self.errorList
			return None
		

	def get_tableThs(self, soup:BeautifulSoup|None=None) ->list[str]:
		""" get table theads """
		if not soup : 
			soup =self.soup
		web상_ths = [ th.text.strip() for th in soup.find_all('th') ]
		db상_ths = []
		for th_name in web상_ths:
			if th_name in self.th_db:
				db상_ths.append( self.th_db[th_name] )
			else:
				db상_ths.append( th_name )

		return db상_ths  
	
	def get_tableTds(self, soup:BeautifulSoup|None=None ) -> list[dict[str, str]]:
		if not soup : soup =self.soup
		result_list =[]
		try:
			table = soup.find('table')
			table_body = table.find('tbody')
			table_trs = table_body.find_all('tr')
			
			for tr in table_trs:
				tds_dict = {}
				for th, td in zip(self.ths, tr.find_all('td')):
					# 모든 공백, 탭, 개행 문자 등을 제거
					cleaned_text = td.text.strip().replace('\r', '').replace('\n', '').replace('\t', '')
					# 연속된 공백을 하나의 공백으로 대체
					cleaned_text = ' '.join(cleaned_text.split())
					tds_dict[th] = cleaned_text
					tds_dict['링크'] = self.get_href( a = tr.find('a', href=True) )
				result_list.append( tds_dict )
			
			return result_list
		except Exception as e:
			self.errorList.append(f" Anayalis Error: {e}")
			return []
		# global scraping_results
		# scraping_results.append({self.url : result })
		# return result
	
	def get_href(self, a:BeautifulSoup|None=None) -> str:
		if a is None : return '#'
		try:
			#### onclick 경우, 
			if a['href'] == '#':
				###  goTo.view('list','13931','134','0402010000'); return false;
				if a['onclick'] is not None and len(a['onclick']) > 5:
					match self.config.get('gov_name').strip() :
						case '한국승강기안전공단':
							link1 = a['onclick'].replace('(', '__').replace(')', '__').split('__')[1]
							link_list = link1.split(',')
							mId = link_list[3].strip("'")
							bIdx = link_list[1].strip("'")
							ptIdx = link_list[2].strip("'")
							return f"mId={mId}&bIdx={bIdx}&ptIdx={ptIdx}"
						
						case '한국소방안전원':
							link1 = a['onclick'].replace('(', '__').replace(')', '__').split('__')[1]
							link_list = link1.split(',')
							postsSeqno = link_list[0].strip("'")
							postsRnum = link_list[1].strip("'")

							return f"&postsSeqno={postsSeqno}&postsRnum={postsRnum}&searchCondition=title&searchKeyword=&pageIndex=1"
						
						case '_':
							return '#'
				else:
					return '#'

			elif ';jsessionid=' in a['href']:
				linkList = a['href'].split(';')
				return linkList[0]+'?'+linkList[1].split('?')[-1]
				조정된link = linkList[1].split('?')
			else:
				#### 대한 산업안전협회 경우 https://www.safety.or.kr/
				#### /safety/bbs/BMSR00207/view.do;jsessionid=E183C56293052018F959EDB82EFBF652?boardId=229015&menuNo=200082&pageIndex=1&searchCondition=&searchKeyword=
				### 에서 ;jsessionid=E183C56293052018F959EDB82EFBF652 삭제함
				
				return a['href'] if bool(a['href'])  else '#'
		except Exception as e:
			self.errorList.append(f"href find error:{e}")
			return '#'