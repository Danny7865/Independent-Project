package tpch.model;

public class UpdateEvent<T> {
    public enum Op { INSERT, DELETE }
    public Op op;
    public T tuple;

    public UpdateEvent() {}
    public UpdateEvent(Op op, T tuple) {
        this.op = op;
        this.tuple = tuple;
    }
}
