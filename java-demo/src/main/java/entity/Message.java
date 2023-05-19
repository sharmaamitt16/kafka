package entity;

public class Message {

  private Integer partition;
  private Long offset;
  private String messageBody;
  private String topic;

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public void setPartition(Integer partition) {
    this.partition = partition;
  }

  public Long getOffset() {
    return offset;
  }

  public void setOffset(Long offset) {
    this.offset = offset;
  }

  public String getMessageBody() {
    return messageBody;
  }

  public void setMessageBody(String messageBody) {
    this.messageBody = messageBody;
  }

  public Message(String t, Integer p, Long o, String m) {
    topic = t;
    partition = p;
    offset = o;
    messageBody = m;
  }

}
