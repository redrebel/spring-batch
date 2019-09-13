package cjred.example.springbatch;

import java.util.List;

public class Movie {
    private String title;

    private long year;

    private List cast;

    private List genres;

    public String getTitle() {
        return title;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public void setCast(List cast) {
        this.cast = cast;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List getGenres() {
        return genres;
    }

    public void setGenres(List genres) {
        this.genres = genres;
    }
}
