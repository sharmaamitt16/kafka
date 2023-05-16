package kafka.javademo.controllers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Check if site is up or not. This is for Spring setup test.
 */
@RestController
public class UrlCheckController {

  private final String SITE_IS_UP = "Site is up!";
  private final String SITE_IS_DOWN = "Site is down!";
  private final String INCORECT_URL = "URL is incorrect!";

  @GetMapping("/check")
  public String getUrlStatusMessage(@RequestParam String url) {
    String returnMessage = "";

    try {
      URL urlObj = new URL(url);
      HttpURLConnection connection = (HttpURLConnection) urlObj.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      int responseCode = connection.getResponseCode() / 100;
      if (responseCode == 2 || responseCode == 3) {
        returnMessage = SITE_IS_UP;
      } else {
        returnMessage = SITE_IS_DOWN;
      }
    } catch (MalformedURLException e) {
      returnMessage = INCORECT_URL;
    } catch (IOException e) {
      returnMessage = SITE_IS_DOWN;
    }

    return returnMessage;
  }

}
