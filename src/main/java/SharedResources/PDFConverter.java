package SharedResources;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;
import org.apache.pdfbox.tools.ExtractText;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class PDFConverter {

    public static final short ToImage = 1;
    public static final short ToHTML = 2;
    public static final short ToText = 4;

    public String convert(short convertion, String path) throws IOException {
        String outputPath = "";
        if (!path.toLowerCase().endsWith(".pdf")){
            System.out.println("Error: File is not PDF!");
            throw new IOException("Error: File is not PDF.");
        }
        try {
            if ((convertion & ToImage) == ToImage)
                outputPath = toImage(path);
            if ((convertion & ToHTML) == ToHTML)
                outputPath = toHTML(path);
            if ((convertion & ToText) == ToText)
                outputPath = toText(path);
        }catch(IOException e){
            throw new IOException("Error: Failed to convert pdf file");
        }
        return outputPath;
    }

    private String toImage(String pdfAbsolutePath) throws IOException {
        PDDocument document = PDDocument.load(new File(pdfAbsolutePath));
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int page = 0; //What page to render (only one at a time)
        BufferedImage bim = pdfRenderer.renderImageWithDPI(page, 300, ImageType.RGB);
        String output = pdfAbsolutePath.substring(0, pdfAbsolutePath.length()-3)+"png";
        ImageIOUtil.writeImage(bim, output, 300);
        document.close();
        System.out.println("Converted PDF to PNG file.");
        return output;
    }

    private String toText(String pdfAbsolutePath) throws IOException {
        String textFileOutput = pdfAbsolutePath.substring(0, pdfAbsolutePath.length()-3) + "txt";
        String[] arguments = {"-endPage", "1", pdfAbsolutePath, textFileOutput};
        ExtractText.main(arguments);
        System.out.println("Converted PDF to txt file.");
        return textFileOutput;
    }

    private String toHTML(String pdfAbsolutePath) throws IOException {
        String htmlFileOutput = pdfAbsolutePath.substring(0, pdfAbsolutePath.length()-3) + "html";
        String[] arguments = {"-html", "-endPage", "1", pdfAbsolutePath, htmlFileOutput};
        ExtractText.main(arguments);
        System.out.println("Converted PDF to HTML file.");
        return htmlFileOutput;
    }

}