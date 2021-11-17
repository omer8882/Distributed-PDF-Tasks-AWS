public class S3Manager {
    private String identifier;
    public S3Manager(String identifier) {
        this.identifier = identifier;
    }

    public String download(String path) {
        System.out.println("Downloading from s3 on path :"+path);
        return "S3Content";
    }
}
