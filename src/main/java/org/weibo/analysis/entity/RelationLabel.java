package org.weibo.analysis.entity;

public enum RelationLabel {
    Author,
    Fans,
    BelongTo,
    At,
    Mentioned,
    ReplyOf,
    Control;

    public String getLabel() {
        return this.name();
    }
}
