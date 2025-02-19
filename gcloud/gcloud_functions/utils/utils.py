import datetime

class UtilsConfigurator:
    def timeframe_window(self, start_date=None, end_date=None):
        """
        Method to get start and end date. 
        Args:
            start_date (datetime)
            end_date (datetime)
        Returns:
            By default returns today and date 30 days from today.
            start_date (datetime)
            end_date (datetime)

        """
        if not start_date:
            start = datetime.datetime.now()
        if not end_date:
            end = start + datetime.timedelta(days=30)

        return start, end
    
    
