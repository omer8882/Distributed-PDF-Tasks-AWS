import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello, World!");
        String pdfAbsolutePath = "//Users/nivdan//Downloads//distri//sample.pdf";
        PDFComverter converter = new PDFComverter();

        short action = PDFComverter.ToImage | PDFComverter.ToHTML | PDFComverter.ToText;
        converter.convertPDF(action, pdfAbsolutePath);
        LocalApplication localApplication =  new LocalApplication();
        localApplication.run("pathToFile", true, 10);

    }
}