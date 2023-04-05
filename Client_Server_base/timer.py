from threading import Timer

##Lets make our timer run in intervals  
##Put it into a class  
##Making it run until we stop it  
##Just getting crazy.Notice We have multiple timers at once!  
class RepeatTimer(Timer):  
    def run(self):  
        while not self.finished.wait(self.interval):  
            self.function(*self.args,**self.kwargs)  
            print(' ')  
##We are now creating a thread timer and controling it  
