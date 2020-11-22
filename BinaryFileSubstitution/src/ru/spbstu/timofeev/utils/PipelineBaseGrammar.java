package ru.spbstu.timofeev.utils;

import ru.spbstu.pipeline.BaseGrammar;

public class PipelineBaseGrammar extends BaseGrammar {

    protected PipelineBaseGrammar(String[] tokens) {
        super(tokens);
    }

    public boolean containsToken(String token) {
        for (int i = 0; i < numberTokens(); ++i) {
            if (token(i).equals(token)) {
                return true;
            }
        }

        return false;
    }
}
