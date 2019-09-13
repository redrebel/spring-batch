package cjred.example.springbatch;

public class MovieGenre {
    private String genre;

    public String getGenre() {
        return genre;
    }

    public String getTitle() {
        return title;
    }

    private String title;

    public MovieGenre(String title, String genre) {
        this.genre = genre;
        this.title = title;
    }
}
