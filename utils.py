import datetime
import logging
from logging.handlers import RotatingFileHandler


class UtilsConfigurator:
    def timeframe_window(self) -> (datetime, datetime): # type: ignore
        # Start day -> today | End Day -> month from today
        # start = datetime.datetime.now()
        # end = start + datetime.timedelta(days=30)

        start = datetime.datetime(2021, 1, 1)
        end = start + datetime.timedelta(days = 1200)

        return start, end
    
