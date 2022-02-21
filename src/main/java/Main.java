import LocalApp.LocalApplication;
import Manager.Manager;
import Worker.Worker;
import com.sun.corba.se.spi.orbutil.threadpool.Work;

public class Main {

    public static void main(String[] args){
        new LocalApplication(args);
    }
}