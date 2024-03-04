LOG_FIELD_MAPPING_FILE_PATH = './config/cf_realtime_log_field_mappings.json'
CF_REALTIME_LOG_FIELDS_LABEL = 'cf_realtime_log_fields'
SAMPLE_DATA_FILE_PATH = './test.json'
SAVE_TO_DRIVE = 'D:\\'
LOGS_DIRECTORY_NAME = 'cloudfront_logs'
LOG_FILE_NAME_PREFIX = 'log_'
LOG_FILE_EXTENSION = '.log'
CS_HEADER_LABEL = 'cs-headers'
CS_HEADER_NAME_LABEL = 'cs-header-names'
CS_IP = 'cs-ip'
CS_METHOD = 'cs-method'
CS_URI = 'cs-uri'
STATUS = 'status'
SC_BYTES = 'sc-bytes'
TIME_TAKEN = 'time-taken'
CS_REFERER = 'cs(Referer)'
CS_USER_AGENT = 'cs(User-Agent)'
CS_COOKIE = 'cs(Cookie)'
X_DISID = 'x-disid'
X_HEADERSIZE = 'x-headersize'
CACHED = 'cached'
CS_RANGE = 'cs(Range)'
X_TCWAIT = 'x-tcwait'
X_TCPINFO_RTT = 'x-tcpinfo_rtt'
X_TCPINFO_RTTVAR = 'x-tcpinfo_rttvar'
X_TCPINFO_SND_CWND = 'x-tcpinfo_snd_cwnd'
X_TCPINFO_RCV_SPACE = 'x-tcpinfo_rcv_space'
X_TDWAIT = 'x-tdwait'
SC_CONTENT_TYPE = 'sc(Content-Type)'
CS_VERSION = 'cs-version'
C_CITY = 'c-city'
C_STATE = 'c-state'
C_COUNTRY = 'c-country'
LOG_FILE_HEADER_CONTENT = "#Version: 1.0\n#Software: 1.0\n#Fields: date time cs-ip cs-method cs-uri status sc-bytes time-taken cs(Referer) cs(User-Agent) cs(Cookie) x-disid x-headersize cached cs(Range) x-tcwait x-tcpinfo_rtt x-tcpinfo_rttvar x-tcpinfo_snd_cwnd x-tcpinfo_rcv_space x-tdwait sc(Content-Type) cs-version c-city c-state c-country\n"
MAPPED_FIELDS = {
    "date": "",
    "time": "",
    "cs-ip": "c-ip", #mapped with c-ip need to clarify and update
    "cs-method": "cs-method",
    "cs-uri": "cs-uri-stem",
    "status": "sc-status",
    "sc-bytes": "sc-bytes",
    "time-taken": "time-taken",
    "cs(Referer)": "cs-referer",
    "cs(User-Agent)": "cs-user-agent",
    "cs(Cookie)": "cs-cookie",
    "x-disid": "",
    "x-headersize": "",
    "cached": "",
    "cs(Range)": "",
    "x-tcwait": "",
    "x-tcpinfo_rtt": "",
    "x-tcpinfo_rttvar": "",
    "x-tcpinfo_snd_cwnd": "",
    "x-tcpinfo_rcv_space": "",
    "x-tdwait": "",
    "sc(Content-Type)": "sc-content-type",
    "cs-version": "cs-protocol-version",
    "c-city": "",
    "c-state": "",
    "c-country": "c-country"
}