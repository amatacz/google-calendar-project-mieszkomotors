import datetime

class UtilsConfigurator:
    def timeframe_window(self, days=30) -> (datetime, datetime): # type: ignore
        # Start day -> today | End Day -> month from today
        start = datetime.datetime.now()
        end = start + datetime.timedelta(days=days)

        return start, end
    
    
