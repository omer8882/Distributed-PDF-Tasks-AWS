import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello, World!");
        String pdfAbsolutePath = "C:\\Users\\Omer\\Desktop\\Secure_Mining_of_Association_Rules_in_Horizontally_Distributed_Databases.pdf";
        PDFComverter converter = new PDFComverter();

        short action = PDFComverter.ToImage | PDFComverter.ToHTML | PDFComverter.ToText;
        converter.convertPDF(action, pdfAbsolutePath);
    }
}