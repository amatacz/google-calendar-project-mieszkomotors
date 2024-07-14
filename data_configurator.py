import datetime

class DataConfigurator:

    def timeframe_window(self) -> (datetime, datetime): # type: ignore
        # Start day -> today | End Day -> month from today
        # start = datetime.datetime.now()
        # end = start + datetime.timedelta(days=30)

        start = datetime.datetime(2021, 6, 30)
        end = start + datetime.timedelta(days = 1110)

        return start, end