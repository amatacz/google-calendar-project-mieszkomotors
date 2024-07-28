import datetime

class UtilsConfigurator:
    def timeframe_window(self) -> (datetime, datetime): # type: ignore
        # Start day -> today | End Day -> month from today
        start = datetime.datetime.now()
        end = start + datetime.timedelta(days=30)

        return start, end
    
