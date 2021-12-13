package hello;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;

import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;
 
import java.io.IOException;
import java.time.Instant;


import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@SpringBootApplication
@RestController
public class Application {

    static class WriteCommittedStream {

        final JsonStreamWriter jsonStreamWriter;
    
        public WriteCommittedStream(String projectId, String datasetName, String tableName) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
    
          try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
    
            WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
            TableName parentTable = TableName.of(projectId, datasetName, tableName);
            CreateWriteStreamRequest createWriteStreamRequest =
                    CreateWriteStreamRequest.newBuilder()
                            .setParent(parentTable.toString())
                            .setWriteStream(stream)
                            .build();
    
            WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);
    
            jsonStreamWriter = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build();
          }
        }
    
        public ApiFuture<AppendRowsResponse> send(Arena arena) {
          Instant now = Instant.now();
          JSONArray jsonArray = new JSONArray();
    
          arena.state.forEach((url, playerState) -> {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("x", playerState.x);
            jsonObject.put("y", playerState.y);
            jsonObject.put("direction", playerState.direction);
            jsonObject.put("wasHit", playerState.wasHit);
            jsonObject.put("score", playerState.score);
            jsonObject.put("player", url);
            jsonObject.put("timestamp", now.getEpochSecond() * 1000 * 1000);
            jsonArray.put(jsonObject);
          });
    
          return jsonStreamWriter.append(jsonArray);
        }
    
      }
    
      final String projectId = ServiceOptions.getDefaultProjectId();
      final String datasetName = "snowball";
      final String tableName = "events";
    
      final WriteCommittedStream writeCommittedStream;
    
      public Application() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        writeCommittedStream = new WriteCommittedStream(projectId, datasetName, tableName);
      }
    

  static class Self {
    public String href;
  }

  static class Links {
    public Self self;
  }

  static class PlayerState {
    public Integer x;
    public Integer y;
    public String direction;
    public Boolean wasHit;
    public Integer score;
  }

  static class Arena {
    public List<Integer> dims;
    public Map<String, PlayerState> state;
  }

  static class ArenaUpdate {
    public Links _links;
    public Arena arena;
  }

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.initDirectFieldAccess();
  }

  @GetMapping("/")
  public String index() {
    return "Let the battle begin!";
  }

  // private String lastCommand = "F";
  private Boolean wasThrown = false;
  private final Integer MAX_SNOWBALL_REACH = 3;

  private Boolean checkThrowCondition(PlayerState myState, Map<String, PlayerState> otherState) {
    switch (myState.direction) {
        case "N":
            return !otherState.entrySet()
                .stream()
                .filter(s -> s.getValue().x.equals(myState.x) && myState.y - s.getValue().y <= MAX_SNOWBALL_REACH)
                .collect(Collectors.toList())
                .isEmpty();
        case "S":
            return !otherState.entrySet()
                .stream()
                .filter(s -> s.getValue().x.equals(myState.x) && s.getValue().y - myState.y <= MAX_SNOWBALL_REACH)
                .collect(Collectors.toList())
                .isEmpty();
        case "E":
            return !otherState.entrySet()
                .stream()
                .filter(s -> s.getValue().y.equals(myState.y) && s.getValue().x - myState.x <= MAX_SNOWBALL_REACH)
                .collect(Collectors.toList())
                .isEmpty();
        case "W":
            return !otherState.entrySet()
                .stream()
                .filter(s -> s.getValue().y.equals(myState.y) && myState.x - s.getValue().x <= MAX_SNOWBALL_REACH)
                .collect(Collectors.toList())
                .isEmpty();
        default: 
            return false;
    }
  }

  @PostMapping("/**")
  public String index(@RequestBody ArenaUpdate arenaUpdate) {
    System.out.println(arenaUpdate);

    // arenaUpdate.arena.state.keySet().stream().forEach(System.out::println);

    Self myLink = arenaUpdate._links.self;
    PlayerState myState = arenaUpdate.arena.state.get(myLink.href);
    arenaUpdate.arena.state.remove(myLink.href);

    writeCommittedStream.send(arenaUpdate.arena);
    
    if (checkThrowCondition(myState, arenaUpdate.arena.state)) {
        return "T";
    } else {
        return "R";
    }

    // String[] commands = new String[]{"F", "R", "L", "T"};
    // int i = new Random().nextInt(4);
    // return commands[i];
  }

}

