import argparse, csv, json, logging, queue, random
import signal, string, sys, threading, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
  
DEFAULT_NAMES = 10
DEFAULT_LENGTH = 4
DEFAULT_BIRTHDAY = "1999-04-20"
DEFAULT_OUTPUT = "valid.txt"
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF = 1.5
DEFAULT_TIMEOUT = 10
DEFAULT_DELAY = 0.1
DEFAULT_THREADS = 5
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    GRAY = "\033[90m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_session(retries=DEFAULT_MAX_RETRIES, backoff_factor=DEFAULT_BACKOFF, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist, allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

class UserAgentRotator:
    def __init__(self, agents=DEFAULT_USER_AGENTS):
        self.agents = agents
        self.lock = threading.Lock()
        self.index = 0

    def get_next(self):
        with self.lock:
            ua = self.agents[self.index % len(self.agents)]
            self.index += 1
            return ua

class ProxyRotator:
    def __init__(self, proxies=None):
        self.proxies = proxies or []
        self.lock = threading.Lock()
        self.index = 0

    def get_next(self):
        if not self.proxies:
            return None
        with self.lock:
            proxy = self.proxies[self.index % len(self.proxies)]
            self.index += 1
            return {"http": proxy, "https": proxy}

def load_proxies_from_file(filepath):
    proxies = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    return proxies

class UsernameGenerator:
    def __init__(self, length, mode="random", alphabet=string.ascii_lowercase + string.digits,
                 prefix="", suffix="", start_from=None):
        self.length = length
        self.mode = mode
        self.alphabet = alphabet
        self.prefix = prefix
        self.suffix = suffix
        self.total_possible = len(alphabet) ** length
        self.generated = set()
        if mode == "systematic":
            if start_from is not None:
                if len(start_from) != length:
                    raise ValueError("start_from length must equal length")
                self.product_iter = self._iter_from(start_from)
            else:
                self.product_iter = product(alphabet, repeat=length)
        else:
            self.generated = set()

    def _iter_from(self, start):
        for tup in product(self.alphabet, repeat=self.length):
            if "".join(tup) >= start:
                yield tup

    def next(self):
        if self.mode == "systematic":
            try:
                return self.prefix + "".join(next(self.product_iter)) + self.suffix
            except StopIteration:
                return None
        else:
            while True:
                core = "".join(random.choice(self.alphabet) for _ in range(self.length))
                username = self.prefix + core + self.suffix
                if username not in self.generated:
                    self.generated.add(username)
                    return username
                if len(self.generated) >= self.total_possible:
                    return None

class Worker:
    def __init__(self, args):
        self.args = args
        self.found = 0
        self.attempts = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.running = True
        self.session = create_session(retries=args.max_retries, backoff_factor=args.backoff_factor)
        self.ua_rotator = UserAgentRotator(args.user_agents if args.user_agents else DEFAULT_USER_AGENTS)
        self.proxy_rotator = ProxyRotator(args.proxies)
        self.resume_set = set()
        if args.resume:
            try:
                with open(args.resume, "r", encoding="utf-8") as f:
                    for line in f:
                        name = line.strip()
                        if name:
                            self.resume_set.add(name)
                logger.info(f"Loaded {len(self.resume_set)} usernames to skip from {args.resume}")
            except FileNotFoundError:
                logger.warning(f"Resume file {args.resume} not found, starting fresh")
        self.output_lock = threading.Lock()
        self.output_handlers = self._init_output_handlers()
        signal.signal(signal.SIGINT, self._signal_handler)

    def _init_output_handlers(self):
        fmt = self.args.output_format
        if fmt == "json":
            return {"json": open(self.args.output + ".json", "w", encoding="utf-8")}
        elif fmt == "csv":
            f = open(self.args.output + ".csv", "w", newline="", encoding="utf-8")
            writer = csv.writer(f)
            writer.writerow(["username", "timestamp"])
            return {"csv": (f, writer)}
        else:
            return {"txt": open(self.args.output, "a", encoding="utf-8")}

    def _signal_handler(self, sig, frame):
        logger.info("Interrupt received, finishing pending tasks...")
        self.running = False

    def write_result(self, username):
        with self.lock:
            self.found += 1
        with self.output_lock:
            fmt = self.args.output_format
            if fmt == "json":
                self.output_handlers["json"].write(json.dumps({"username": username, "timestamp": time.time()}) + "\n")
                self.output_handlers["json"].flush()
            elif fmt == "csv":
                f, writer = self.output_handlers["csv"]
                writer.writerow([username, time.time()])
                f.flush()
            else:
                self.output_handlers["txt"].write(username + "\n")
                self.output_handlers["txt"].flush()
        msg = f"{bcolors.OKBLUE}[{self.found}/{self.args.names}] [+] Found: {username}{bcolors.ENDC}"
        print(msg)

    def process_username(self, username):
        if username in self.resume_set:
            return False
        self.attempts += 1
        try:
            available = self.check_username(username)
            if available:
                self.write_result(username)
                return True
            else:
                if self.args.verbose:
                    print(f"{bcolors.FAIL}[-] {username} taken{bcolors.ENDC}")
                else:
                    print(f"{bcolors.GRAY}[-] {username} taken{bcolors.ENDC}", end="\r")
                return False
        except Exception as e:
            self.errors += 1
            logger.error(f"Error checking {username}: {e}")
            return False

    def check_username(self, username):
        url = "https://auth.roblox.com/v1/usernames/validate"
        params = {"request.username": username, "request.birthday": self.args.birthday}
        headers = {"User-Agent": self.ua_rotator.get_next(), "Accept": "application/json"}
        proxies = self.proxy_rotator.get_next()
        response = self.session.get(url, params=params, headers=headers, proxies=proxies, timeout=self.args.timeout)
        response.raise_for_status()
        data = response.json()
        return data.get("code") == 0

    def run(self, generator):
        with ThreadPoolExecutor(max_workers=self.args.threads) as executor:
            futures = []
            pending = 0
            with tqdm(total=self.args.names, disable=not TQDM_AVAILABLE, desc="Valid found") as pbar:
                while self.running and self.found < self.args.names:
                    if len(futures) < self.args.threads * 2:
                        username = generator.next()
                        if username is None:
                            logger.warning("No more usernames to generate")
                            break
                        future = executor.submit(self.process_username, username)
                        futures.append(future)
                        pending += 1
                    if pending >= self.args.threads:
                        done, futures = futures[0], futures[1:]
                        pending -= 1
                        try:
                            result = done.result(timeout=0.1)
                            if result:
                                pbar.update(1)
                        except Exception:
                            pass
                    time.sleep(self.args.delay)
                for future in futures:
                    try:
                        result = future.result(timeout=1)
                        if result:
                            pbar.update(1)
                    except Exception:
                        pass
    def close(self):
        for handle in self.output_handlers.values():
            if hasattr(handle, "close"):
                handle.close()
def main():
    parser = argparse.ArgumentParser(description="Roblox username checker")
    parser.add_argument("-n", "--names", type=int, default=DEFAULT_NAMES, help="Number of valid usernames to find")
    parser.add_argument("-l", "--length", type=int, default=DEFAULT_LENGTH, help="Length of username core")
    parser.add_argument("-b", "--birthday", default=DEFAULT_BIRTHDAY, help="Birthday (YYYY-MM-DD)")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT, help="Output file base name")
    parser.add_argument("-d", "--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests")
    parser.add_argument("--mode", choices=["random", "systematic"], default="random", help="Generation mode")
    parser.add_argument("--alphabet", default=string.ascii_lowercase + string.digits, help="Characters to use")
    parser.add_argument("--prefix", default="", help="Prefix to add")
    parser.add_argument("--suffix", default="", help="Suffix to add")
    parser.add_argument("--start", help="Starting point for systematic mode (must match length)")
    parser.add_argument("--proxies", nargs="+", help="List of proxies (http://user:pass@ip:port)")
    parser.add_argument("--proxy-file", help="File with proxies (one per line)")
    parser.add_argument("--user-agents", nargs="+", help="Custom User-Agent strings")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries")
    parser.add_argument("--backoff-factor", type=float, default=DEFAULT_BACKOFF, help="Backoff factor")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Request timeout")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS, help="Number of concurrent threads")
    parser.add_argument("--output-format", choices=["txt", "json", "csv"], default="txt", help="Output format")
    parser.add_argument("--resume", help="File with already checked usernames to skip")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    parser.add_argument("--log-file", help="Log to file")
    args = parser.parse_args()
    if args.quiet:
        logger.setLevel(logging.WARNING)
    if args.log_file:
        fh = logging.FileHandler(args.log_file)
        fh.setLevel(logging.DEBUG if args.verbose else logging.INFO)
        logger.addHandler(fh)
    if args.proxy_file:
        proxies = load_proxies_from_file(args.proxy_file)
        if args.proxies:
            proxies.extend(args.proxies)
    else:
        proxies = args.proxies
    generator = UsernameGenerator(args.length, args.mode, args.alphabet, args.prefix, args.suffix, args.start)
    worker = Worker(args)
    try:
        worker.run(generator)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        worker.close()
        logger.info(f"Finished. Found {worker.found} valid usernames out of {worker.attempts} attempts. Errors: {worker.errors}")
        if worker.found > 0:
            print(f"{bcolors.OKBLUE}[!] Results saved to {args.output}{bcolors.ENDC}")

if __name__ == "__main__":
    main()
