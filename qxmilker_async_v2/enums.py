from enum import Flag, auto

class AUTH_STATE(Flag):
    NOT_AUTHORIZED = auto() # Didn't send ssid yet
    AUTH_REJECTED = auto() # ssid rejected
    AUTH_SUCCESS = auto() # ssid accepted
    AUTH_FAILURE = auto()

class MESSAGE(Flag):
    SENT = auto()
    RECVD = auto()

class STATE(Flag):
    INITIALIZED = auto()
    RUNNING = auto()
    CLOSED = auto()