package rest;

public class MainCallAPIWikidata {
    public static void main(String[] args) throws Exception {
        CallAPIWikidata callWiki = new CallAPIWikidata();
        callWiki.getPropertyFromId();
    }
}