lexer grammar HogQLLexer;

@members {

private static boolean isAsciiAlpha(int character) {
    return character >= 0 && character < 128 &&
            ((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z'));
}

private static boolean isAsciiAlphanumeric(int character) {
    return isAsciiAlpha(character) || (character >= '0' && character <= '9');
}

private static boolean isAsciiWhitespace(int character) {
    return character == ' ' || character == '\t' || character == '\n' ||
            character == '\u000B' || character == '\f' || character == '\r';
}

private int skipWhitespaceAndComments(int index) {
    while (true) {
        int character = _input.LA(index);
        if (isAsciiWhitespace(character)) {
            index++;
            continue;
        }

        if (character == '/' && _input.LA(index + 1) == '/') {
            index += 2;
        }
        else if (character == '-' && _input.LA(index + 1) == '-') {
            index += 2;
        }
        else if (character == '#') {
            index++;
        }
        else {
            return index;
        }

        while (true) {
            character = _input.LA(index);
            if (character <= 0 || character == '\n' || character == '\r') {
                break;
            }
            index++;
        }
    }
}

private boolean isOpeningTag() {
    int firstCharacter = _input.LA(1);
    if (!isAsciiAlpha(firstCharacter) && firstCharacter != '_') {
        return false;
    }

    int index = 2;
    while (true) {
        int character = _input.LA(index);
        if (isAsciiAlphanumeric(character) || character == '_' || character == '-') {
            index++;
        }
        else {
            break;
        }
    }

    int character = _input.LA(index);
    if (character == '>' || character == '/') {
        return true;
    }

    if (isAsciiWhitespace(character)) {
        index = skipWhitespaceAndComments(index + 1);
        character = _input.LA(index);
        return isAsciiAlphanumeric(character) || character == '_' || character == '>' || character == '/';
    }

    return false;
}

}
