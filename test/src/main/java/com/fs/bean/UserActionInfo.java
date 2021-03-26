package com.fs.bean;



public class UserActionInfo {
    private int userId;
    private long itemId;
    private int category;
    private String behavior;
    private long timestamp;

    public UserActionInfo(int userId, long itemId, int category, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.category = category;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserActionInfo{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", category=" + category +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
