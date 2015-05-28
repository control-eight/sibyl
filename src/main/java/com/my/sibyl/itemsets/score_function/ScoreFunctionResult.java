package com.my.sibyl.itemsets.score_function;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author abykovsky
 * @since 1/24/15
 */
public class ScoreFunctionResult<T> implements Serializable {

    private final T result;

    private final Map<String, Number> measures = new HashMap<>();

    public ScoreFunctionResult(T result, double confidence, double lift) {
        this.result = result;
        this.measures.put("confidence", confidence);
        this.measures.put("lift", lift);
    }

    public T getResult() {
        return result;
    }

    public Map<String, Number> getMeasures() {
        return measures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScoreFunctionResult that = (ScoreFunctionResult) o;

        if (result != null ? !result.equals(that.result) : that.result != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return result != null ? result.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ScoreFunctionResult{" +
                "result=" + result +
                ", measures=" + measures +
                '}';
    }
}
