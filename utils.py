import datetime
import logging
from logging.handlers import RotatingFileHandler


class UtilsConfigurator:
    def timeframe_window(self) -> (datetime, datetime): # type: ignore
        # Start day -> today | End Day -> month from today
        # start = datetime.datetime.now()
        # end = start + datetime.timedelta(days=30)

        start = datetime.datetime(2023, 6, 30)
        end = start + datetime.timedelta(days = 100)

        return start, end
    
