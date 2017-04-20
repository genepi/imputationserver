package genepi.imputationserver.util;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

public class PasswordCreator {

	public static String createPassword() {
		return createPassword(5, 5, 2, 2, 3);
	}

	public static String createPassword(int uppercaseLetters, int lowercaseLetters, int numbers, int symbols, int duplicates) {

		String pwd = null;

		do {
			String upper = RandomStringUtils.random(uppercaseLetters, 0, 0, true, false, null, new SecureRandom())
					.toUpperCase();

			String lower = RandomStringUtils.random(lowercaseLetters, 0, 0, true, false, null, new SecureRandom())
					.toLowerCase();

			String number = RandomStringUtils.random(numbers, 0, 0, false, true, null, new SecureRandom())
					.replaceAll("\\s+", "");

			// 33: exclude space; also includes letters
			String symbol = RandomStringUtils.random(symbols, 33, 127, false, false, null, new SecureRandom());

			pwd = shuffleAndCheck(upper + lower + number + symbol, duplicates);

		} while (pwd == null);

		return pwd;

	}

	private static String shuffleAndCheck(String input, int duplicates) {
		List<Character> characters = new ArrayList<Character>();
		int countDuplicates = 0;
		for (char c : input.toCharArray()) {
			if (characters.contains(c)) {
				countDuplicates++;
			}
			characters.add(c);
		}
		if (countDuplicates >= duplicates) {
			return null;
		}

		StringBuilder pwd = new StringBuilder(input.length());
		while (characters.size() != 0) {
			int randPicker = (int) (Math.random() * characters.size());
			pwd.append(characters.remove(randPicker));
		}
		return pwd.toString();
	}

}
