package mnix.kafka;

import com.fatboyindustrial.gsonjavatime.InstantConverter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.Instant;

public class GsonFactory {
    public static final Gson GSON = new GsonBuilder().registerTypeAdapter(Instant.class, new InstantConverter()).create();
}
