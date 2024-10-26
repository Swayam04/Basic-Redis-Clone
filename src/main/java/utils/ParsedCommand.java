package utils;

import java.util.List;

public record ParsedCommand(String name, List<String> args) {}
