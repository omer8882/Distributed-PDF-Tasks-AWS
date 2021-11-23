import java.io.IOException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello, World!");
        String pdfAbsolutePath = "//Users/nivdan//Downloads//distri//sample.pdf";
        PDFConverter converter = new PDFConverter();

        short action = PDFConverter.ToImage | PDFConverter.ToHTML | PDFConverter.ToText;
        converter.convert(action, pdfAbsolutePath);
        LocalApplication localApplication =  new LocalApplication();
        localApplication.run("pathToFile" ,10, true);
//        Region region = Region.US_EAST_1;
//        Ec2Client  ec2Client = Ec2Client.builder().region(region).build();




    }
}