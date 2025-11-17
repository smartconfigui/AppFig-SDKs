using UnityEngine;
using UnityEngine.UI;
using TMPro;

/// <summary>
/// AppFigApplier - Simple component for applying AppFig feature flags to Unity components
///
/// USAGE: Attach to a GameObject, set the feature name and expected value, then configure
/// what should change when the feature matches.
///
/// EXAMPLE - White Square to Black:
/// - Feature Name: "skin_theme"
/// - Expected Value: "black"
/// - Target Image: Your square's Image component
/// - Override Color: Black
/// - Default Color: White
/// Result: Square is black when skin_theme == "black", otherwise white
///
/// PERFORMANCE: Checks feature values every frame (cached dictionary lookup - zero cost).
/// Only applies changes when the feature value actually changes.
/// </summary>
[AddComponentMenu("AppFig/AppFig Applier")]
public class AppFigApplier : MonoBehaviour
{
    [Header("Feature Configuration")]
    [Tooltip("The AppFig feature name to check, e.g. 'skin_theme'")]
    public string featureName = "skin_theme";

    [Tooltip("The value to match for applying the override, e.g. 'black' or 'retro'")]
    public string expectedValue = "black";

    [Header("Color Override")]
    [Tooltip("Target UI Image or SpriteRenderer to change color if value matches")]
    public Image targetImage;
    public SpriteRenderer targetSpriteRenderer;

    [Tooltip("Color to apply if the feature value matches")]
    public Color overrideColor = Color.black;

    [Tooltip("Color to apply if the feature value does not match")]
    public Color defaultColor = Color.white;

    [Header("Sprite Override")]
    [Tooltip("Override sprite when value matches")]
    public Sprite overrideSprite;

    [Tooltip("Default sprite when value doesn't match")]
    public Sprite defaultSprite;

    [Header("GameObject Toggle")]
    [Tooltip("Target GameObject to enable/disable based on match")]
    public GameObject targetObject;

    [Tooltip("Enable target when value matches (true) or when it doesn't match (false)")]
    public bool enableOnMatch = true;

    [Header("Text Content")]
    [Tooltip("Target Text/TextMeshPro to update")]
    public Text targetText;
    public TextMeshProUGUI targetTMPText;

    [Tooltip("Text to display when value matches")]
    public string overrideText = "";

    [Tooltip("Text to display when value doesn't match")]
    public string defaultText = "";

    private string lastFeatureValue = null;

    void Update()
    {
        if (string.IsNullOrEmpty(featureName)) return;

        string actualValue = AppFig.GetFeatureValue(featureName);

        if (actualValue == lastFeatureValue) return;
        lastFeatureValue = actualValue;

        bool isMatch = actualValue == expectedValue;

        // Apply color to Image
        if (targetImage != null)
        {
            targetImage.color = isMatch ? overrideColor : defaultColor;
        }

        // Apply color to SpriteRenderer
        if (targetSpriteRenderer != null)
        {
            targetSpriteRenderer.color = isMatch ? overrideColor : defaultColor;
        }

        // Apply sprite to Image
        if (targetImage != null && (overrideSprite != null || defaultSprite != null))
        {
            targetImage.sprite = isMatch ? overrideSprite : defaultSprite;
        }

        // Apply sprite to SpriteRenderer
        if (targetSpriteRenderer != null && (overrideSprite != null || defaultSprite != null))
        {
            targetSpriteRenderer.sprite = isMatch ? overrideSprite : defaultSprite;
        }

        // Enable/Disable GameObject
        if (targetObject != null)
        {
            bool shouldBeActive = enableOnMatch ? isMatch : !isMatch;
            if (targetObject.activeSelf != shouldBeActive)
            {
                targetObject.SetActive(shouldBeActive);
            }
        }

        // Apply text content
        if (targetText != null)
        {
            targetText.text = isMatch ? overrideText : defaultText;
        }

        if (targetTMPText != null)
        {
            targetTMPText.text = isMatch ? overrideText : defaultText;
        }
    }
}
