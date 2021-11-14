package distributed;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        LocalApplication localApplication =  new LocalApplication();
        localApplication.run("pathToFile", true, 10);

    }
}