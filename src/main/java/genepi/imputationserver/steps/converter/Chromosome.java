package genepi.imputationserver.steps.converter;

public class Chromosome {

	public static int CHR01 = 249250621;
	public static int CHR02 = 243199373;
	public static int CHR03 = 198022430;
	public static int CHR04 = 191154276;
	public static int CHR05 = 180915260;
	public static int CHR06 = 171115067;
	public static int CHR07 = 159138663;
	public static int CHR08 = 146364022;
	public static int CHR09 = 141213431;
	public static int CHR10 = 135534747;
	public static int CHR11 = 135006516;
	public static int CHR12 = 133851895;
	public static int CHR13 = 115169878;
	public static int CHR14 = 107349540;
	public static int CHR15 = 102531392;
	public static int CHR16 = 90354753;
	public static int CHR17 = 81195210;
	public static int CHR18 = 78077248;
	public static int CHR19 = 59128983;
	public static int CHR20 = 63025520;
	public static int CHR21 = 48129895;
	public static int CHR22 = 51304566;
	public static int CHRX = 155270560;
	public static int CHRY = 59373566;
	public static int CHRMT = 16569;

	public static int UNDEFINED = 0;

	public static int getChrLength(String chromosome) {

		switch (chromosome) {
		case "1":
			return CHR01;
		case "2":
			return CHR02;
		case "3":
			return CHR03;
		case "4":
			return CHR04;
		case "5":
			return CHR05;
		case "6":
			return CHR06;
		case "7":
			return CHR07;
		case "8":
			return CHR08;
		case "9":
			return CHR09;
		case "10":
			return CHR10;
		case "11":
			return CHR11;
		case "12":
			return CHR12;
		case "13":
			return CHR13;
		case "14":
			return CHR14;
		case "15":
			return CHR15;
		case "16":
			return CHR16;
		case "17":
			return CHR17;
		case "18":
			return CHR18;
		case "19":
			return CHR19;
		case "20":
			return CHR20;
		case "21":
			return CHR21;
		case "22":
			return CHR22;
		case "X":
			return CHRX;
		case "Y":
			return CHRY;
		case "MT":
			return CHRMT;
		}
		return UNDEFINED;
	}

}