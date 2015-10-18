import os, sys, traceback, thread, urlparse, time, csv, codecs, urllib2, imghdr
import sqlite3 as lite
from threading import Thread
from Queue import LifoQueue
from threading import active_count as threading_active_count
from lxml import etree, html
import HTMLParser
from posixpath import basename
import socks
from sockshandler import SocksiPyHandler


m_db = None

m_failed_urls_filename = "failed_urls.txt"
m_profiles_filename = "profiles_done.db"
m_csv_filename = "results.csv"

m_log_file_name = os.path.basename(__file__) + ".log"
m_num_workers = 1
m_socket_timeout = 15
m_retry_timeout = 5
m_retries_num = 20
m_worker_timeout = 5


m_imgdir_path = "images"

m_csv_col_names = ["app_name", "app_release_date", "author", "price", "img_filename", "url"]

m_socks_proxy_base_port = 0 #if 0 socks proxy not used
m_socks_host = "127.0.0.1"


class dbstuff:
	m_db_name = ""

	def __init__(self, dbname, csv_file_name, csv_col_names, failed_urls_filename):
		self.m_db_name = dbname
		self.m_db_lock = thread.allocate_lock() #Condition()

		self.m_csv_file = open(csv_file_name, 'a+')
		self.m_csv_writer = csv.writer(self.m_csv_file)

		csv_size = os.path.getsize(csv_file_name)
		if csv_size==0:
			self.m_csv_writer.writerow(csv_col_names)

		self.m_total_done = 0

		self.failed_urls_filename = failed_urls_filename
		with open(self.failed_urls_filename, 'w'):
			pass

	def csv_flush(self):
		self.m_csv_file.flush()

	def close(self):
		if not self.db_con is None:
			self.db_con.commit()
			self.db_con.close()
		if not self.m_csv_file is None:
			self.m_csv_file.close()


	def csv_save_item(self, item, url_fingerprint, encode=True):

		try:

			self.m_db_lock.acquire()

			if self.db_exist_entry_no_lock(url_fingerprint):
				return

			for i in range(len(item)):
				if item[i]=="--":
					item[i]=''

			if encode:
				self.m_csv_writer.writerow([s.encode("utf-8") for s in item])
			else:
				self.m_csv_writer.writerow(item)

			self.sql_add_url_fingerprint_no_lock(url_fingerprint)

			self.m_csv_file.flush()

			self.m_total_done +=1

		finally:
			self.m_db_lock.release()

		return

	def db_create(self):

		self.db_con = lite.connect(self.m_db_name, check_same_thread=False)

		cur = self.db_con.cursor()
		cur.execute(
				"CREATE TABLE IF NOT EXISTS Entries (Fingerprint Text PRIMARY KEY)")

		return

	def lock_acquire(self):
		self.m_db_lock.acquire()

	def lock_release(self):
		self.m_db_lock.release()

	def sql_add_url_fingerprint_no_lock(self, Fingerprint):

		try:
			cur = self.db_con.cursor()

			cur.execute("INSERT INTO Entries(Fingerprint) \
										VALUES(?)", (Fingerprint,))
		except Exception as ex:
			log(str(ex))
			return False

		return True

	def db_exist_entry_no_lock(self, fingerprint):

		cur = self.db_con.cursor()

		cur.execute("Select * from Entries where Fingerprint=?", (fingerprint,))

		data = cur.fetchone()
		if (data and data[0]):
			return True

		return False

	def db_exist_entry(self, fingerprint):
		res = None

		try:
			self.m_db_lock.acquire()

			res = self.db_exist_entry_no_lock(fingerprint)

		finally:
			self.m_db_lock.release()

		return res

	def add_failed_url(self, url):

		with open(self.failed_urls_filename, "a") as f:
			f.write(url + "\r\n")

		return




class MyLogger():

	def __init__(self):

		self.console_stream = sys.stdout
		self.filestream = None
		return

	def __init_file_logger(self, log_file_name):

		if not self.filestream is None:
			return

		try:
			self.filestream = codecs.open(log_file_name, mode='w+', encoding='utf-8')
		except:
			self.filestream = None

		return

	def write_log(self, str, truncate=False):

		str = str + "\r\n"
		if self.console_stream:
			self.console_stream.write(str.encode("utf-8"))

		if self.filestream:
			if truncate:
				self.filestream.seek(0)
				self.filestream.truncate()
			self.filestream.write(str)
			self.filestream.flush()
		return

	@staticmethod
	def init_logger(logger_file_name=""):
		global root_logger

		if logger_file_name:
			root_logger.__init_file_logger(logger_file_name)

		return

	@staticmethod
	def log(*args):
		if (not root_logger):
			return

		args1 = []

		for j in args:
			try:
				if isinstance(j, unicode):
					args1.append(j)
				elif isinstance(j, str):
					args1.append(unicode(j, 'utf-8'))
				else:
					args1.append(unicode(j))
			except Exception as ex:
				ss = str(ex)
				pass

		root_logger.write_log(''.join(args1))
		return



root_logger = MyLogger()

log = root_logger.log




class HtmlTool(object):

	h = HTMLParser.HTMLParser()

	@staticmethod
	def get_attrib(tree, xpath, checkSingle=False):
		res = tree.xpath(xpath)
		if checkSingle:
			if len(res) != 1:
				return ""

		if len(res) and (res[0] != "" and not res[0] is None):
			return res[0]

		return ""

	@staticmethod
	def get_single_result_text(tree, xpath, checkSingle=False):
		res = tree.xpath(xpath)
		if checkSingle:
			if len(res) != 1:
				return ""
		if len(res) and (res[0] != "" and not res[0] is None):
			return HtmlTool.get_text_content(etree.tostring(res[0]))

		return ""

	@staticmethod
	def get_text_content(content):

		if not len(content.strip()):
			return ""

		tree = html.fromstring(content)

		return HtmlTool.__clean_text(tree.text_content())


	@staticmethod
	def __clean_text(text, preserveNewLines=False):
		cleanText = text

		if preserveNewLines == False:
			cleanText = cleanText.replace("\t", "").replace("\r", "").replace("\n", " ")

		cleanText = " ".join(cleanText.split())
		return cleanText



def get_page_content(url, socks_port = 0,  log_prefix=""):

	for i in range(m_retries_num):

		content = get_page_content_try(url, socks_port, log_prefix)
		if content:
			return content

		log(log_prefix, "ERROR get_page_content, retrying ", url)
		time.sleep(m_retry_timeout)


	log(log_prefix, "FATAL ERROR get_page_content ", url)

	return ""


def get_page_content_try(url, socks_port = 0, log_prefix = ""):

	try:

		if socks_port:
			opener = urllib2.build_opener(SocksiPyHandler(socks.PROXY_TYPE_SOCKS5, m_socks_host, socks_port))
			req = opener.open(url, timeout= m_socket_timeout)
		else:
			req = urllib2.urlopen(url, timeout= m_socket_timeout)

		content = req.read()

	except Exception as ex:

		log(log_prefix, "request failed ", url)
		log(log_prefix, str(ex))
		return ""


	return content




class Monitor(Thread):
	def __init__(self, queue, db):
		Thread.__init__(self)
		self.queue = queue
		self.finish_signal = False
		self.db = db

	def finish(self):
		self.finish_signal = True

	def run(self):

		while not self.finish_signal:

			time.sleep(5)

			if self.finish_signal:
				log("Monitor exit")
				break

			log("Elements in Queue: ", self.queue.qsize(), " Active Threads: ", threading_active_count(), " Total done: ", self.db.m_total_done)


class Worker(Thread):

	def __init__(self, queue, socks_proxy_port, thread_id, db):
		Thread.__init__(self)
		self.queue = queue
		self.socks_proxy_port = socks_proxy_port
		self.worker_index = "worker" + str(thread_id)
		self.db = db
		self.log_prefix = self.worker_index + ": "

	def run(self):
		try:
			while True:
				queue_item = self.queue.get()
				if queue_item == None:
					self.queue.put(None) # Notify the next worker
					log(self.worker_index, " no more tasks, exit")
					break

				if m_worker_timeout:
					time.sleep(m_worker_timeout)

				iter_count = 0

				if len(queue_item) == 2:
					(url, callback) = queue_item
				else:
					(url, callback, iter_count) = queue_item

				callback(url, self.queue, self, iter_count)

				self.queue.task_done()
		except Exception as ex:
			log(str(ex), "FATAL ERROR worker")

		return



def is_cat_ok(tree):

	res = tree.xpath("//div[@id='genre-nav']")
	if res is None or len(res)!=1:
		return  False
	return True


def parse_main(url, queue, worker, iter_count):

	page = get_page_content(url, worker.socks_proxy_port, worker.log_prefix)
	if not page:
		log(worker.log_prefix, "ERROR get_page_content ", url)
		worker.db.add_failed_url(url)
		return

	tree = etree.HTML(page)

	if not is_cat_ok(tree):
		if iter_count<10:
			queue.put((url, parse_main, iter_count+1))
			log(worker.log_prefix, "FATALL retrying#",str(iter_count), " ", url)
		else:
			worker.db.add_failed_url(url)
			log(worker.log_prefix, "FATALL err ", url)
		return

	cats = tree.xpath("//div[@id='genre-nav']//ul[contains(@class,'list column first')]/li/a[contains(@class,'top-level-genre')]")
	for cat in cats:
		cur_url = HtmlTool.get_attrib(cat, "@href")
		cur_url = urlparse.urljoin(url, cur_url)

		queue.put((cur_url, parse_category))

	return



def parse_category(url, queue, worker, iter_count):

	page = get_page_content(url, worker.socks_proxy_port, worker.log_prefix)
	if not page:
		log(worker.log_prefix, "ERROR get_page_content ", url)
		worker.db.add_failed_url(url)
		return


	tree = etree.HTML(page)

	if not is_cat_ok(tree):
		if iter_count<10:
			queue.put((url, parse_category, iter_count+1))
			log(worker.log_prefix, "FATALL retrying#",str(iter_count), " ", url)
		else:
			worker.db.add_failed_url(url)
			log(worker.log_prefix, "FATALL err ", url)
		return

	subcats = tree.xpath("//div[@id='genre-nav']//ul[contains(@class,'list column first')]/li//ul[contains(@class,'top-level-subgenres')]/li/a")
	for s in subcats:

		cur_url = HtmlTool.get_attrib(s, "@href")
		cur_url = urlparse.urljoin(url, cur_url)

		queue.put((cur_url, parse_subcategory))

	parse_subcategory(url, queue, worker, iter_count, tree)

	return

def parse_subcategory(url, queue, worker, iter_count, tree = None):
	'''
	:param url:
	:param queue:
	:param worker:
	:param iter_count:
	:param tree: by default tree is None, but when called from category tree is not None, so no need to retrieve page again
	:return:
	'''

	if tree is None:

		page = get_page_content(url, worker.socks_proxy_port, worker.log_prefix)
		if not len(page):
			log(worker.log_prefix, "ERROR get_page_content ", url)
			worker.db.add_failed_url(url)
			return

		tree = etree.HTML(page)

		if not is_cat_ok(tree):
			if iter_count<10:
				queue.put((url, parse_subcategory, iter_count+1))
				log(worker.log_prefix, "FATALL retrying#",str(iter_count), " ", url)
			else:
				worker.db.add_failed_url(url)
				log(worker.log_prefix, "FATALL err ", url)
			return

	cat_letters = tree.xpath("//div[@id='selectedgenre']/ul[contains(@class,'list alpha')]/li/a")
	for c in cat_letters:
		cur_url = HtmlTool.get_attrib(c, "@href")
		cur_url = urlparse.urljoin(url, cur_url)

		queue.put((cur_url, parse_cat_letter))

	return

def parse_cat_letter(url, queue, worker, iter_count):

	page = get_page_content(url, worker.socks_proxy_port, worker.log_prefix)
	if not page:
		log(worker.log_prefix, "ERROR get_page_content ", url)
		worker.db.add_failed_url(url)
		return

	tree = etree.HTML(page)

	if not is_cat_ok(tree):
		if iter_count<10:
			queue.put((url, parse_cat_letter, iter_count+1))
			log(worker.log_prefix, "FATALL retrying#",str(iter_count), " ", url)
		else:
			worker.db.add_failed_url(url)
			log(worker.log_prefix, "FATALL err ", url)
		return


	nextpage = tree.xpath("//div[@id='selectedgenre']/ul[contains(@class,'list paginate')][1]/li[a[contains(@class,'selected')]]/following-sibling::li[1]/a")
	if not nextpage is None and len(nextpage):
		cur_url = HtmlTool.get_attrib(nextpage[0], "@href")
		cur_url = urlparse.urljoin(url, cur_url)

		queue.put((cur_url, parse_cat_letter))


	apps = tree.xpath("//div[@id='selectedcontent']/div[contains(@class,'column')]/ul/li/a")
	for a in apps:
		cur_url = HtmlTool.get_attrib(a, "@href")

		if not cur_url.startswith("http"):
			cur_url = urlparse.urljoin(url, cur_url)
			log("")
			log(worker.log_prefix, "urljoin ", cur_url)

		app_id = get_app_id_from_url(cur_url)

		if not len(app_id):
			log(worker.log_prefix, "ERROR wrong app_id ", cur_url)
			worker.db.add_failed_url(cur_url)
			continue


		if worker.db.db_exist_entry(app_id):
			continue

		queue.put((cur_url, parse_app))


	return

def parse_app(url, queue, worker, iter_count):
	try:
		parse_app1(url,queue, worker, iter_count)
	except Exception as ex:
		log(worker.log_prefix, "ERROR parse_app ", url)
		log(str(ex))
		worker.db.add_failed_url(url)
	return


def get_app_id_from_url(url):
	parse_object = urlparse.urlparse(url)
	app_id = basename(parse_object.path)
	if not app_id.startswith("id"):
		return ""

	return app_id

def parse_app1(url, queue, worker, iter_count):

	app_id = get_app_id_from_url(url)
	if not app_id:
		log(worker.log_prefix, "FATAL ERROR wrong app_id ", url)
		worker.db.add_failed_url(url)
		return

	if worker.db.db_exist_entry(app_id):
		return

	page = get_page_content(url, worker.socks_proxy_port, worker.log_prefix)
	if not page:
		log(worker.log_prefix, "FATAL ERROR get_page_content ", url)
		worker.db.add_failed_url(url)
		return

	tree = etree.HTML(page)

	app_name = HtmlTool.get_single_result_text(tree,"//div[@id='content']//div[@id='title']//h1[@itemprop='name']", True)

	if not app_name:
		if iter_count<10:
			queue.put((url, parse_app, iter_count+1))
			log(worker.log_prefix, "FATALL retrying#",str(iter_count), " ", url)
		else:
			worker.db.add_failed_url(url)
			log(worker.log_prefix, "FATALL err ", url)
		return



	app_release_date = ""
	dd = HtmlTool.get_attrib(tree,"//div[@id='left-stack']//li[contains(@class,'release-date')]//span[@itemprop='datePublished']/@content", True)
	if not dd is None:
		dd = dd.split(" ")
		if len(dd)==3:
			app_release_date = dd[0]
		else:
			log(worker.log_prefix, "ERROR app_release_date ", " ".join(dd), " ", url)

	author = HtmlTool.get_single_result_text(tree,"//div[@id='left-stack']//*[@itemprop='author']/*[@itemprop='name']", True)
	price = HtmlTool.get_single_result_text(tree,"//div[@id='left-stack']//*[@itemprop='offers']/*[@itemprop='price']", True)

	img_url = HtmlTool.get_attrib(tree,"//div[@id='left-stack']//*[@itemprop='image']/@content", True)
	img_file_name = ""
	if img_url:

		img_content = get_page_content(img_url, worker.socks_proxy_port, worker.log_prefix)
		if not len(img_content):
			log(worker.log_prefix, "FATAL ERROR download img ", url)
			worker.db.add_failed_url(url)
			return

		img_file_name = app_id
		img_file_path = os.path.join(m_imgdir_path, img_file_name)

		with open(img_file_path, 'wb') as jpgFile:
			jpgFile.write(img_content)

		imgtype = imghdr.what(img_file_path)
		extension = ""
		if imgtype=="jpeg":
			extension = ".jpg"
		elif imgtype=="png":
			extension = ".png"

		if not extension:
			log(worker.log_prefix, "FATAL ERROR uknown img format ", url)
			worker.db.add_failed_url(url)
			os.remove(img_file_path)
			return

		img_file_name = img_file_name + extension

		new_img_path = os.path.join(m_imgdir_path, img_file_name)

		if worker.db.db_exist_entry(app_id):
			os.remove(img_file_path)
			return

		os.rename(img_file_path, new_img_path)

	#["app_name", "app_release_date", "author", "price", "img_filename", "url"]
	item = [
		app_name,
		app_release_date,
		author,
		price,
		img_file_name,
		url
	]
	worker.db.csv_save_item(item, app_id)

	return




def do_job():

	m_db = dbstuff(m_profiles_filename, m_csv_filename, m_csv_col_names, m_failed_urls_filename)
	m_db.db_create()


	m_workers = []
	m_queue = LifoQueue()

	monitor = Monitor(m_queue, m_db)
	monitor.start()

	if not os.path.exists(m_imgdir_path):
		os.makedirs(m_imgdir_path)



	for i in range(m_num_workers):

		worker = Worker(
			m_queue,
			socks_proxy_port=m_socks_proxy_base_port+i if m_socks_proxy_base_port else 0,
			thread_id=i+1,
			db= m_db)

		m_workers.append(worker)


	for w in m_workers:
		w.start()

	starturl = "https://itunes.apple.com/us/genre/ios-books/id6018?mt=8"
	m_queue.put((starturl, parse_main))


	log("waiting workers")

	m_queue.join()
	log("all tasks done")

	#Send a 'signal' to workers to finish
	m_queue.put(None)

	for w in m_workers:
		w.join()

	log("job finished")
	monitor.finish()

	m_db.close()

	return


if __name__ == "__main__":


	try:

		abs_dir = os.path.abspath(os.path.dirname(__file__))
		os.chdir(abs_dir)

		MyLogger.init_logger(m_log_file_name)

		do_job()

	except Exception as ex:
		log("EXCEPTION: " + str(ex))
		log(traceback.format_exc())
